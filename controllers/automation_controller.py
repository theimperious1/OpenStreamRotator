import time
import obsws_python as obs
import logging
import os
import shutil
import signal
import asyncio
import json
from typing import Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from core.video_registration_queue import VideoRegistrationQueue
from managers.playlist_manager import PlaylistManager
from managers.stream_manager import StreamManager
from controllers.obs_controller import OBSController
from managers.platform_manager import PlatformManager
from services.notification_service import NotificationService
from playback.playback_tracker import PlaybackTracker
from playback.playback_skip_detector import PlaybackSkipDetector
from services.twitch_live_checker import TwitchLiveChecker
from handlers.rotation_handler import RotationHandler
from handlers.override_handler import OverrideHandler
from handlers.content_switch_handler import ContentSwitchHandler
from handlers.temp_playback_handler import TempPlaybackHandler
from utils.video_processor import kill_all_running_processes as kill_processor_processes

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# OBS Configuration
OBS_HOST = os.getenv("OBS_HOST", "127.0.0.1")
OBS_PORT = int(os.getenv("OBS_PORT", 4455))
OBS_PASSWORD = os.getenv("OBS_PASSWORD", "")
SCENE_LIVE = os.getenv("SCENE_LIVE", "Pause screen")
SCENE_OFFLINE = os.getenv("SCENE_OFFLINE", "Stream")
SCENE_CONTENT_SWITCH = os.getenv("SCENE_CONTENT_SWITCH", "content-switch")
VLC_SOURCE_NAME = os.getenv("VLC_SOURCE_NAME", "Playlist")

# Twitch Configuration (used for live checker)
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "")

# Discord Configuration
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")


class AutomationController:
    """Main automation controller - orchestrates all components."""

    def __init__(self):
        # Core managers
        self.db = DatabaseManager()
        self.config_manager = ConfigManager()
        
        # Thread-safe queue for video registration from background downloads
        self.video_registration_queue = VideoRegistrationQueue()
        
        self.playlist_manager = PlaylistManager(self.db, self.config_manager, self.video_registration_queue)

        # OBS
        self.obs_client: Optional[obs.ReqClient] = None
        self.obs_controller: Optional[OBSController] = None
        
        # Platforms
        self.platform_manager = PlatformManager()
        self.stream_manager: Optional[StreamManager] = None

        # Services
        self.notification_service = NotificationService(DISCORD_WEBHOOK_URL)
        self.playback_tracker = PlaybackTracker(self.db)
        self.playback_skip_detector: Optional[PlaybackSkipDetector] = None
        
        # Twitch live checker
        if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
            self.twitch_live_checker = TwitchLiveChecker(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
        else:
            self.twitch_live_checker = None

        # Handlers (initialized in _initialize_handlers)
        self.rotation_handler: Optional[RotationHandler] = None
        self.override_handler: Optional[OverrideHandler] = None
        self.content_switch_handler: Optional[ContentSwitchHandler] = None
        self.temp_playback_handler: Optional[TempPlaybackHandler] = None

        # Executor for background downloads
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="download_worker")

        # State
        self.current_session_id = None
        self.next_prepared_playlists = None
        self.last_stream_status = None
        self.is_rotating = False
        self.override_already_cleaned = False
        self._pending_seek_position_ms = 0
        self._seek_retry_count = 0
        self._rotation_postpone_logged = False
        self._downloads_triggered_this_rotation = False  # Track if downloads already triggered after rotation
        self._just_resumed_session = False  # Track if we just resumed to skip initial download trigger
        self._skip_detector_init_delay = 0  # Delay skip detector initialization after override to let VLC reload
        self._skip_rotation_check_delay = 0  # Delay rotation checks after override restoration to let VLC stabilize
        self.shutdown_event = False
        
        # Background download state (thread-safe communication from background thread)
        self._pending_db_playlists_to_initialize = None  # Playlists to initialize in DB
        self._pending_db_playlists_to_complete = None    # Playlists to mark as COMPLETED in DB
        
        # Override preparation async coordination (Phase 1: New flags)
        self._override_preparation_pending = False  # Override queued, background prep running
        self._override_prep_ready = False  # Background prep completed, ready for commit
        self._override_prep_data = {}  # Data from queue phase (override_pending_folder path, etc)
        self._background_download_in_progress = False  # Any background download active (rotation, override, auto-resume)

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received...")
        kill_processor_processes()
        logger.info("Cleanup complete. Setting shutdown flag...")
        self.shutdown_event = True

    def _initialize_handlers(self):
        """Initialize all handler objects after OBS and services are ready."""
        assert self.obs_controller is not None, "OBS controller must be initialized before handlers"
        
        self.rotation_handler = RotationHandler(
            self.db, self.config_manager, self.playlist_manager,
            self.playback_skip_detector, self.notification_service,
            self.playback_tracker
        )
        self.override_handler = OverrideHandler(
            self.db, self.config_manager, self.playlist_manager,
            self.notification_service, self.playback_tracker
        )
        self.content_switch_handler = ContentSwitchHandler(
            self.db, self.config_manager, self.playlist_manager,
            self.obs_controller, self.notification_service
        )
        self.stream_manager = StreamManager(self.platform_manager)
        
        # Initialize temp playback handler (needs stream_manager)
        self.temp_playback_handler = TempPlaybackHandler(
            self.db, self.config_manager, self.playlist_manager,
            self.obs_controller, self.stream_manager
        )
        # Set up callbacks for coordination
        self.temp_playback_handler.set_callbacks(
            check_manual_override=self.check_manual_override,
            auto_resume_downloads=self._auto_resume_pending_downloads,
            initialize_skip_detector=self._initialize_skip_detector,
            get_background_download_in_progress=lambda: self._background_download_in_progress,
            set_background_download_in_progress=lambda v: setattr(self, '_background_download_in_progress', v),
            set_override_prep_ready=lambda v: setattr(self, '_override_prep_ready', v)
        )
        
        logger.info("Handlers initialized successfully")

    def save_playback_on_exit(self):
        """Save current playback position when program exits."""
        self.playback_tracker.save_on_exit(self.current_session_id, self.obs_controller)

    def auto_save_playback_position(self):
        """Auto-save playback position for power loss resilience."""
        self.playback_tracker.auto_save_position(self.current_session_id, self.obs_controller)

    def connect_obs(self) -> bool:
        """Connect to OBS WebSocket."""
        try:
            self.obs_client = obs.ReqClient(host=OBS_HOST, port=OBS_PORT, password=OBS_PASSWORD, timeout=3)
            self.obs_controller = OBSController(self.obs_client)
            logger.info("Connected to OBS successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to OBS: {e}")
            return False

    def setup_platforms(self):
        """Initialize enabled streaming platforms."""
        self.platform_manager.setup(self.twitch_live_checker)

    def start_rotation_session(self, manual_playlists=None) -> bool:
        """Start a new rotation session."""
        assert self.rotation_handler is not None, "Rotation handler not initialized"
        
        logger.info("Starting new rotation session...")
        self.rotation_handler.reset_rotation_log_flag()

        settings = self.config_manager.get_settings()
        next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')

        # Use prepared playlists or select new ones
        using_prepared = False
        if self.next_prepared_playlists:
            playlists = self.next_prepared_playlists
            self.next_prepared_playlists = None
            using_prepared = True
            logger.info(f"Using prepared playlists: {[p['name'] for p in playlists]}")
        else:
            playlists = self.playlist_manager.select_playlists_for_rotation(manual_playlists)
        
        if not playlists:
            logger.error("No playlists selected for rotation")
            self.notification_service.notify_rotation_error("No playlists available")
            return False

        # Download if not already prepared
        if not using_prepared:
            logger.info(f"Downloading {len(playlists)} playlists...")
            self.notification_service.notify_rotation_started([p['name'] for p in playlists])
            
            # Check for verbose yt-dlp logging
            settings = self.config_manager.get_settings()
            verbose_download = settings.get('yt_dlp_verbose', False)
            
            download_result = self.playlist_manager.download_playlists(playlists, next_folder, verbose=verbose_download)
            total_duration_seconds = download_result.get('total_duration_seconds', 0)

            if not download_result.get('success'):
                logger.error("Failed to download all playlists")
                self.notification_service.notify_download_warning(
                    "Some playlists failed to download, continuing with available content"
                )
        else:
            logger.info("Using pre-downloaded playlists, skipping download step")
            total_duration_seconds = 0
            for playlist in playlists:
                playlist_id = playlist.get('id')
                if playlist_id:
                    videos = self.db.get_videos_by_playlist(playlist_id)
                    for video in videos:
                        total_duration_seconds += video.get('duration_seconds', 0)

        # Validate and create session
        if not self.playlist_manager.validate_downloads(next_folder):
            logger.error("Download validation failed")
            self.notification_service.notify_rotation_error("Download validation failed")
            return False

        playlist_names = [p['name'] for p in playlists]
        stream_title = self.playlist_manager.generate_stream_title(playlist_names)
        
        if total_duration_seconds == 0:
            rotation_hours = settings.get('rotation_hours', 12)
            total_duration_seconds = rotation_hours * 3600
            logger.info(f"Using config rotation_hours: {rotation_hours}h")
        
        current_time = datetime.now()
        estimated_finish_time = current_time + timedelta(seconds=total_duration_seconds)
        
        logger.info(f"Total rotation duration: {total_duration_seconds}s (~{total_duration_seconds // 60} minutes)")
        logger.info(f"Estimated finish: {estimated_finish_time}")

        playlist_ids = [p['id'] for p in playlists]
        self.current_session_id = self.db.create_rotation_session(
            playlist_ids, stream_title,
            total_duration_seconds=total_duration_seconds,
            estimated_finish_time=estimated_finish_time,
            download_trigger_time=None
        )
        # Keep temp playback handler in sync
        if self.temp_playback_handler:
            self.temp_playback_handler.set_session_id(self.current_session_id)

        self.playback_tracker.reset()
        logger.info("Rotation session prepared, ready to switch")
        return True

    async def execute_content_switch(self, is_override_resumption: bool = False) -> bool:
        """Execute content switch using handler."""
        assert self.content_switch_handler is not None, "Content switch handler not initialized"
        assert self.stream_manager is not None, "Stream manager not initialized"
        
        if not self.obs_controller:
            logger.error("OBS controller not initialized")
            return False

        logger.info(f"Executing content switch (override_resumption={is_override_resumption})")
        self.is_rotating = True

        settings = self.config_manager.get_settings()
        current_folder = settings.get('video_folder', 'C:/stream_videos/')
        next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')

        try:
            # Prepare for switch
            if not self.content_switch_handler.prepare_for_switch(SCENE_CONTENT_SWITCH, VLC_SOURCE_NAME):
                logger.error("Failed to prepare for content switch")
                self.is_rotating = False
                return False

            # Get override info
            is_override_switch = False
            backup_folder = None
            suspended_session = self.db.get_suspended_session()
            if suspended_session and not is_override_resumption:
                # Skip backup if we already cleaned up this override
                if not self.override_already_cleaned:
                    is_override_switch = True
                    suspension_data = json.loads(suspended_session.get('suspension_data', '{}'))
                    backup_folder = suspension_data.get('backup_folder')

            # Execute folder operations
            if not self.content_switch_handler.execute_switch(
                current_folder, next_folder,
                is_override_resumption=is_override_resumption,
                is_override_switch=is_override_switch,
                backup_folder=backup_folder
            ):
                self.is_rotating = False
                return False

            # Finalize (update VLC + switch scene)
            target_scene = SCENE_LIVE if self.last_stream_status == "live" else SCENE_OFFLINE
            finalize_success, vlc_playlist = self.content_switch_handler.finalize_switch(
                current_folder, VLC_SOURCE_NAME, target_scene, SCENE_OFFLINE, self.last_stream_status
            )
            if not finalize_success:
                self.is_rotating = False
                return False

            # Mark playlists as played
            try:
                session = self.db.get_current_session()
                if session:
                    self.content_switch_handler.mark_playlists_as_played(session.get('id'))
            except Exception as e:
                logger.warning(f"Failed to mark playlists as played: {e}")

            # Initialize skip detector
            # This will handle category updates on video transitions
            self._initialize_skip_detector()
            
            # Update skip detector with the new playlist from content switch
            # This ensures playlist tracking stays in sync with what VLC is actually playing
            if self.playback_skip_detector and vlc_playlist:
                self.playback_skip_detector.set_vlc_playlist(vlc_playlist)
                logger.debug(f"Updated skip detector playlist with {len(vlc_playlist)} videos from content switch")
            
            # Process any queued videos from downloads so they're in database before category lookup
            self._process_video_registration_queue()
            
            # Update stream title and category
            try:
                session = self.db.get_current_session()
                if session:
                    stream_title = session.get('stream_title', '')
                    
                    # Get category from first video in rotation (with fallback to first playlist)
                    category = None
                    if self.content_switch_handler:
                        category = self.content_switch_handler.get_initial_rotation_category(
                            self.playback_skip_detector, 
                            self.playlist_manager
                        )
                    
                    await self.stream_manager.update_stream_info(stream_title, category)
                    logger.info(f"Updated stream: title='{stream_title}', category='{category}'")
            except Exception as e:
                logger.warning(f"Failed to update stream metadata: {e}")

            # If temp playback was active, the normal rotation has completed the consolidation
            # Complete temp playback cleanup properly
            if self.temp_playback_handler and self.temp_playback_handler.is_active:
                await self.temp_playback_handler.cleanup_after_rotation()
            
            # Clean up temporary download files from the previous rotation
            # Safe to do now that content switch is complete
            settings = self.config_manager.get_settings()
            pending_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
            self.playlist_manager.cleanup_temp_downloads(pending_folder)
            
            self.is_rotating = False
            self.override_already_cleaned = False
            logger.info("Content switch completed successfully")
            return True

        except Exception as e:
            logger.error(f"Content switch failed: {e}", exc_info=True)
            self.is_rotating = False
            return False

    def _initialize_skip_detector(self):
        """Initialize skip detector for current session."""
        if not self.obs_controller:
            return
        
        settings = self.config_manager.get_settings()
        video_folder = settings.get('video_folder', 'C:/stream_videos/')
        
        if self.playback_skip_detector is None:
            self.playback_skip_detector = PlaybackSkipDetector(
                self.db, self.obs_controller, VLC_SOURCE_NAME, video_folder,
                self.content_switch_handler, self.stream_manager
            )
            # Update rotation handler's reference to the newly created detector
            assert self.rotation_handler is not None, "Rotation handler must be initialized"
            self.rotation_handler.set_playback_skip_detector(self.playback_skip_detector)
            # Update temp playback handler's reference
            if self.temp_playback_handler:
                self.temp_playback_handler.set_skip_detector(self.playback_skip_detector)
        
        session = self.db.get_current_session()
        if not session:
            return
        
        total_duration = session.get('total_duration_seconds', 0)
        playback_seconds = session.get('playback_seconds', 0)
        
        original_finish = None
        finish_time_str = session.get('estimated_finish_time')
        if finish_time_str:
            try:
                original_finish = datetime.fromisoformat(finish_time_str)
            except (ValueError, TypeError):
                pass
        
        resume_position_ms = int(playback_seconds * 1000) if playback_seconds > 0 else 0
        self.playback_skip_detector.initialize(
            total_duration_seconds=total_duration,
            original_finish_time=original_finish,
            resume_position_ms=resume_position_ms
        )

    def _sync_background_download_next_rotation(self, playlists):
        """Synchronous wrapper for executor.
        
        NOTE: This runs in a background thread and must NOT call database methods directly
        due to SQLite's thread safety requirements. Instead, set flags that the main thread
        will process.
        """
        try:
            logger.info(f"Downloading next rotation in thread: {[p['name'] for p in playlists]}")
            settings = self.config_manager.get_settings()
            next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
            
            # Queue database initialization to be done in main thread
            self._pending_db_playlists_to_initialize = [p['name'] for p in playlists]
            
            # Check for verbose yt-dlp logging
            verbose_download = settings.get('yt_dlp_verbose', False)
            
            download_result = self.playlist_manager.download_playlists(playlists, next_folder, verbose=verbose_download)
            
            if download_result.get('success'):
                self.next_prepared_playlists = playlists
                logger.info(f"Background download completed: {[p['name'] for p in playlists]}")
                
                # Queue database status update to be done in main thread
                self._pending_db_playlists_to_complete = [p['name'] for p in playlists]
                
                self.notification_service.notify_next_rotation_ready([p['name'] for p in playlists])
            else:
                logger.warning("Background download had failures")
                self.notification_service.notify_background_download_warning()
        except Exception as e:
            logger.error(f"Background download error: {e}")
            self.notification_service.notify_background_download_error(str(e))
        finally:
            # Clear flag when download completes (success or failure)
            self._background_download_in_progress = False

    def _process_video_registration_queue(self):
        """Process pending videos from background download registration queue.
        
        This method runs in the main thread and safely registers queued videos
        that were discovered by background download threads.
        """
        if not self.video_registration_queue.has_pending_videos():
            return
        
        pending_videos = self.video_registration_queue.get_pending_videos()
        if not pending_videos:
            return
        
        logger.info(f"Processing {len(pending_videos)} queued videos for database registration")
        
        registered_count = 0
        total_duration = 0
        
        for video_data in pending_videos:
            try:
                self.db.add_video(
                    video_data['playlist_id'],
                    video_data['filename'],
                    title=video_data['title'],
                    duration_seconds=video_data['duration_seconds'],
                    file_size_mb=video_data['file_size_mb'],
                    playlist_name=video_data.get('playlist_name')
                )
                registered_count += 1
                total_duration += video_data['duration_seconds']
                logger.debug(f"Registered queued video: {video_data['filename']} ({video_data['duration_seconds']}s)")
            except Exception as e:
                # Check if it's a duplicate constraint error
                if "UNIQUE constraint failed" in str(e) or "already exists" in str(e):
                    logger.debug(f"Video already exists in database: {video_data['filename']}, skipping")
                else:
                    logger.error(f"Error registering queued video {video_data['filename']}: {e}")
        
        if registered_count > 0:
            logger.info(f"Registered {registered_count} queued videos from background download, total: {total_duration}s")

    def _process_pending_database_operations(self):
        """Process database operations queued by background download thread.
        
        This method runs in the main thread and safely applies database changes
        that were set as flags by the background download thread (which cannot
        directly access the database due to SQLite thread safety).
        """
        # Initialize next_playlists if background thread queued it
        if self._pending_db_playlists_to_initialize is not None and self.current_session_id:
            self.db.initialize_next_playlists(self.current_session_id, self._pending_db_playlists_to_initialize)
            self._pending_db_playlists_to_initialize = None
        
        # Mark playlists as COMPLETED if background thread queued it
        if self._pending_db_playlists_to_complete is not None and self.current_session_id:
            self.db.complete_next_playlists(self.current_session_id, self._pending_db_playlists_to_complete)
            self._pending_db_playlists_to_complete = None

    async def check_for_rotation(self):
        """Check if rotation is needed and handle it."""
        assert self.rotation_handler is not None, "Rotation handler not initialized"
        
        if self.is_rotating:
            return

        session = self.db.get_current_session()
        if not session:
            return

        # Check skip detection
        skip_detected, skip_info = self.rotation_handler.check_skip_detection(self.current_session_id)
        if skip_detected and skip_info:
            self.notification_service.notify_playback_skip(
                skip_info["time_skipped_seconds"],
                skip_info["new_finish_time_str"]
            )
            
            # Update stream category based on current video (with proper async/await)
            current_video = skip_info.get("current_video_filename")
            if current_video and self.content_switch_handler and self.stream_manager:
                try:
                    temp_playback_active = bool(self.temp_playback_handler and self.temp_playback_handler.is_active)
                    await self.content_switch_handler.update_category_for_video_async(
                        current_video, self.stream_manager, temp_playback_active=temp_playback_active
                    )
                except Exception as e:
                    logger.warning(f"Failed to update category on video transition: {e}")
        
        # Handle VLC refresh if needed during temp playback
        # The skip detector sets _vlc_refresh_needed flag when new files are available during temp playback
        if (self.playback_skip_detector and 
            self.playback_skip_detector._vlc_refresh_needed and 
            self.temp_playback_handler and self.temp_playback_handler.is_active):
            try:
                logger.info("Executing VLC refresh from skip detector flag")
                await self.temp_playback_handler.refresh_vlc()
                self.playback_skip_detector._vlc_refresh_needed = False
            except Exception as e:
                logger.error(f"Error executing VLC refresh: {e}")
                self.playback_skip_detector._vlc_refresh_needed = False

        # Trigger background download only if pending folder is empty and not already triggered
        # Skip on first loop after resume to avoid downloading when resuming into existing rotation
        settings = self.config_manager.get_settings()
        pending_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
        if (not self._downloads_triggered_this_rotation and 
            self.playlist_manager.is_folder_empty(pending_folder) and 
            not self._background_download_in_progress and
            not self._just_resumed_session):
            
            playlists = self.rotation_handler.trigger_background_download(
                self.next_prepared_playlists, self._background_download_in_progress
            )
            if playlists:
                self._downloads_triggered_this_rotation = True
                self._background_download_in_progress = True
                loop = asyncio.get_event_loop()
                loop.run_in_executor(self.executor, self._sync_background_download_next_rotation, playlists)
                logger.debug("Download triggered (pending folder empty)")
        
        # Clear the resume flag after first loop iteration
        if self._just_resumed_session:
            self._just_resumed_session = False

        # Check if all content is consumed and prepared playlists are ready for immediate rotation
        # Also trigger rotation if there's pending content even if prepared_playlists flag is empty (covers restart scenario)
        # Also trigger rotation if there's a suspended session waiting to be restored (override completion)
        has_pending_content = not self.playlist_manager.is_folder_empty(pending_folder)
        has_suspended_session = self.db.get_suspended_session() is not None
        should_rotate = False
        
        # Check if temp playback should be activated (long playlist being downloaded)
        if (self.playback_skip_detector is not None and 
            self.playback_skip_detector._all_content_consumed and 
            self.temp_playback_handler and not self.temp_playback_handler.is_active and
            not has_suspended_session):
            
            # Check if we have prepared playlists downloading but not completed yet
            next_playlists_raw = session.get('next_playlists', [])
            next_playlists_status_raw = session.get('next_playlists_status', {})
            
            # Parse JSON if needed
            next_playlists = json.loads(next_playlists_raw) if isinstance(next_playlists_raw, str) else (next_playlists_raw or [])
            next_playlists_status = json.loads(next_playlists_status_raw) if isinstance(next_playlists_status_raw, str) else (next_playlists_status_raw or {})
            
            # Get pending folder status
            settings = self.config_manager.get_settings()
            pending_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
            pending_complete_files = self.playlist_manager.get_complete_video_files(pending_folder)
            pending_has_files = len(pending_complete_files) > 0
            
            # Check if prepared playlists are still downloading (not completed)
            pending_incomplete = False
            if next_playlists:
                for playlist_name in next_playlists:
                    # Check status from the next_playlists_status dict
                    playlist_is_complete = next_playlists_status.get(playlist_name) == 'COMPLETED'
                    if not playlist_is_complete:
                        pending_incomplete = True
                        break
            
            # Trigger temp playback if all conditions met
            # But skip if already in temp playback mode - just refresh VLC instead
            if pending_has_files and pending_incomplete and self.temp_playback_handler:
                if self.temp_playback_handler.is_active:
                    logger.info(f"Already in temp playback, new files available ({len(pending_complete_files)} files in pending) - triggering VLC refresh")
                    self.playback_skip_detector._vlc_refresh_needed = True
                else:
                    logger.info(f"Long playlist detected downloading ({len(pending_complete_files)} files ready in pending) - activating temp playback")
                    await self.temp_playback_handler.activate(session)
                return
        
        if self.playback_skip_detector is not None and self.playback_skip_detector._all_content_consumed:
            # Only rotate on pending content if no background download is in progress
            # This prevents triggering rotation mid-download and auto-selecting same playlists
            pending_content_ready = has_pending_content and not self._background_download_in_progress
            should_rotate = self.next_prepared_playlists or pending_content_ready or has_suspended_session
        
        if should_rotate:
            if self.next_prepared_playlists:
                logger.info("All content consumed and prepared playlists ready - triggering immediate rotation")
            elif has_suspended_session:
                logger.info("All content consumed and suspended session exists - triggering override completion/restoration")
            else:
                logger.info("All content consumed and pending content exists - triggering rotation (prepared from previous run)")
            
            total_seconds, has_suspended = self.rotation_handler.get_rotation_completion_info(session)
            self.rotation_handler.log_rotation_completion(total_seconds)
            
            # Reset flag before handling rotation
            assert self.playback_skip_detector is not None, "playback_skip_detector must be initialized"
            self.playback_skip_detector._all_content_consumed = False
            
            # Handle rotation immediately
            if has_suspended:
                await self._handle_override_completion(session)
            else:
                await self._handle_normal_rotation()
            return

        # Check rotation duration
        if not self.rotation_handler.check_rotation_duration(session):
            return

        total_seconds, has_suspended = self.rotation_handler.get_rotation_completion_info(session)
        self.rotation_handler.log_rotation_completion(total_seconds)

        # Handle override completion or normal rotation
        if has_suspended:
            await self._handle_override_completion(session)
        else:
            await self._handle_normal_rotation()

    async def _handle_override_completion(self, session):
        """Handle completion of override rotation."""
        assert self.rotation_handler is not None, "Rotation handler not initialized"
        
        suspended_session = self.db.get_suspended_session()
        if not suspended_session:
            return

        logger.info(f"Override completed, resuming session {suspended_session['id']}")
        
        settings = self.config_manager.get_settings()
        current_folder = settings.get('video_folder', 'C:/stream_videos/')
        
        suspension_data = json.loads(suspended_session.get('suspension_data', '{}'))
        backup_folder = suspension_data.get('backup_folder')
        pending_backup_folder = suspension_data.get('pending_backup_folder')
        prepared_playlist_names = suspension_data.get('prepared_playlist_names', [])

        if self.current_session_id:
            total_seconds = self.playback_tracker.get_total_seconds()
            self.db.update_session_playback(self.current_session_id, total_seconds)
            self.db.end_session(self.current_session_id)

        # Restore original content
        restore_success = self.rotation_handler.restore_after_override(
            current_folder, backup_folder, pending_backup_folder,
            prepared_playlist_names, self.obs_controller, VLC_SOURCE_NAME, SCENE_CONTENT_SWITCH
        )

        # Restore prepared rotation
        if restore_success:
            next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
            restore_prepared_success, restored_playlists = self.rotation_handler.restore_prepared_rotation(
                pending_backup_folder, next_folder, prepared_playlist_names
            )
            if restored_playlists:
                self.next_prepared_playlists = restored_playlists

        # Resume session
        self.db.resume_session(suspended_session['id'])
        self.current_session_id = suspended_session['id']
        # Keep temp playback handler in sync
        if self.temp_playback_handler:
            self.temp_playback_handler.set_session_id(self.current_session_id)
        self.playback_tracker.total_playback_seconds = suspended_session.get('playback_seconds', 0)
        
        original_duration = suspended_session.get('total_duration_seconds', 0)
        elapsed = suspended_session.get('playback_seconds', 0)
        remaining_seconds = original_duration - elapsed
        new_finish_time = datetime.now() + timedelta(seconds=remaining_seconds)
        
        self.db.update_session_times(
            suspended_session['id'],
            new_finish_time.isoformat(),
            ""
        )
        
        logger.info(f"Resumed session {suspended_session['id']}, remaining: {remaining_seconds}s")
        self.notification_service.notify_override_complete([suspended_session.get('stream_title', 'Unknown')])
        
        # Update VLC with restored content
        if self.obs_controller:
            success, playlist = self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, current_folder)
            if not success:
                logger.error("Failed to update VLC source after override restore")
            elif self.playback_skip_detector:
                self.playback_skip_detector.set_vlc_playlist(playlist)
            
            # Switch back to appropriate scene based on stream status
            target_scene = SCENE_LIVE if self.last_stream_status == "live" else SCENE_OFFLINE
            self.obs_controller.switch_scene(target_scene)
            logger.info(f"Switched back to {target_scene} scene after override completion")
        
        self._background_download_in_progress = False
        self._downloads_triggered_this_rotation = False  # Reset flag when resuming original rotation
        
        # Set delays after override restoration:
        # 1. _skip_detector_init_delay: Wait for VLC to reload playlist and seek to resume position
        # 2. _skip_rotation_check_delay: Wait before checking for rotation to prevent premature trigger
        self._skip_detector_init_delay = 5  # 5 iterations of ~0.5s each = ~2.5 seconds
        self._skip_rotation_check_delay = 10  # 10 iterations of ~0.5s each = ~5 seconds (longer buffer)

    async def _handle_normal_rotation(self):
        """Handle normal rotation completion."""
        # Don't rotate if stream is live
        if self.last_stream_status == "live":
            if not self._rotation_postpone_logged:
                logger.info("Stream is live, postponing rotation until stream goes offline")
                self._rotation_postpone_logged = True
            return

        if self.current_session_id:
            # Before ending session, get the current playlist info for audit trail
            session = self.db.get_current_session()
            current_playlist_names = []
            
            if session:
                try:
                    playlists_selected = session.get('playlists_selected', '')
                    if playlists_selected:
                        playlist_ids = json.loads(playlists_selected)
                        playlists = self.playlist_manager.get_playlists_by_ids(playlist_ids)
                        if playlists:
                            current_playlist_names = [p['name'] for p in playlists]
                            # Record what was just played
                            self.db.set_current_playlists(self.current_session_id, current_playlist_names)
                            logger.info(f"Recorded current playlists: {current_playlist_names}")
                except Exception as e:
                    logger.warning(f"Failed to record current playlists: {e}")
            
            total_seconds = self.playback_tracker.get_total_seconds()
            self.db.update_session_playback(self.current_session_id, total_seconds)
            self.db.end_session(self.current_session_id)

        if self.start_rotation_session():
            # Record the new playlists being prepared
            try:
                session = self.db.get_current_session()
                if session and self.next_prepared_playlists:
                    next_playlist_names = [p['name'] for p in self.next_prepared_playlists]
                    self.db.set_next_playlists(session['id'], next_playlist_names)
                    logger.info(f"Recorded next playlists for session {session['id']}: {next_playlist_names}")
            except Exception as e:
                logger.warning(f"Failed to record next playlists: {e}")
            
            # Reset download flag when starting new rotation
            self._downloads_triggered_this_rotation = False
            self._background_download_in_progress = False
            await self.execute_content_switch()

    async def check_manual_override(self) -> bool:
        """
        Check for and process manual overrides.
        
        Uses two-phase approach:
        Phase 1 (Queue): Validate and queue override for background download - returns immediately
        Phase 2 (Commit): Wait for background download + rotation download idle, then commit - blocks until safe
        """
        assert self.override_handler is not None, "Override handler not initialized"
        
        # Phase 2: Check if override prep is ready to commit
        if self._override_prep_ready and not self._background_download_in_progress:
            logger.info("Override preparation ready and rotation downloads idle - committing override")
            
            # Get the override data
            override = self.override_handler.get_active_override()
            if override:
                # Commit the override (swap pending folders, suspend session)
                commit_success = self.override_handler.commit_override_preparation(
                    self.current_session_id,
                    self.next_prepared_playlists,
                    self._override_prep_data,
                    override
                )
                
                if commit_success:
                    # Clear prepared playlists since we're doing override
                    self.next_prepared_playlists = None
                    self._background_download_in_progress = True  # Still downloading override content
                    
                    # Start override rotation
                    settings = self.config_manager.get_settings()
                    next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
                    selected = override.get('selected_playlists', [])
                    
                    if self.override_handler.start_override_rotation(selected, next_folder):
                        if self.start_rotation_session(manual_playlists=selected):
                            await self.execute_content_switch()
                    
                    self._background_download_in_progress = False
                    self._override_prep_ready = False
                    self._override_preparation_pending = False
                    if self.temp_playback_handler:
                        self.temp_playback_handler._override_queued = False  # Clear queue flag when override executes
                    self.override_handler.clear_override()
                    return True
                else:
                    logger.error("Override preparation commit failed")
                    self._override_prep_ready = False
                    self._override_preparation_pending = False
                    return False
            
            return False
        
        # Phase 1: Check if new override triggered
        if not self.override_handler.check_override_triggered():
            return False

        override = self.override_handler.get_active_override()
        if not override or not override.get('trigger_now', False):
            return False

        if not self.override_handler.validate_override(override):
            return False

        # VALIDATE SELECTED PLAYLISTS EARLY, BEFORE ANY FILE OPERATIONS
        selected = override.get('selected_playlists', [])
        self.override_handler.sync_config_playlists()
        all_playlists = self.db.get_enabled_playlists()
        selected_playlist_objs = [p for p in all_playlists if p['name'] in selected]
        
        if not selected_playlist_objs:
            logger.error(f"Override playlists invalid or not found in database: {selected}")
            logger.info("Clearing invalid override without making any changes")
            if self.temp_playback_handler:
                self.temp_playback_handler._override_queued = False  # Clear queue flag when override is invalid
            self.override_handler.clear_override()
            return False

        # CHECK: If temp playback is active, queue the override instead of executing it
        if self.temp_playback_handler and self.temp_playback_handler.is_active:
            logger.info("Temp playback is active - queueing override to execute after temp playback finishes")
            # Queue Phase 1 preparation (needed for Phase 2 to execute later)
            self._override_prep_data = self.override_handler.queue_override_preparation(selected)
            self._override_preparation_pending = True
            self.temp_playback_handler.queue_override()
            # Don't clear override - user may cancel it before temp playback finishes
            return False

        logger.info("Manual override triggered - queuing preparation phase")
        
        settings = self.config_manager.get_settings()
        current_folder = settings.get('video_folder', 'C:/stream_videos/')

        # Check if we're already in an override (there's a suspended original session)
        suspended_session = self.db.get_suspended_session()
        session_to_suspend = self.current_session_id
        
        if suspended_session:
            # We're replacing an active override - the suspended session is the original rotation waiting to resume
            logger.info(f"Replacing active override (session {self.current_session_id}) with new override")
            logger.info(f"Original rotation (session {suspended_session['id']}) will resume after new override completes")
            
            # First, clean up the current override's /live folder content
            try:
                logger.info("Cleaning up current override content")
                if os.path.exists(current_folder):
                    for filename in os.listdir(current_folder):
                        file_path = os.path.join(current_folder, filename)
                        try:
                            if os.path.isfile(file_path):
                                os.unlink(file_path)
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                        except Exception as e:
                            logger.warning(f"Could not delete {filename}: {e}")
                self.override_already_cleaned = True
            except Exception as e:
                logger.error(f"Error cleaning up override content: {e}")
            
            # End the current override session (user doesn't want it anymore)
            if self.current_session_id is not None:
                self.db.end_session(self.current_session_id)
            
            # Resume to the original suspended session after this new override
            session_to_suspend = suspended_session['id']
            logger.info(f"New override will return to original session {session_to_suspend}")
        
        # PHASE 1: QUEUE - Queue preparation for background async download
        self._override_preparation_pending = True
        self._override_prep_ready = False
        self._override_prep_data = self.override_handler.queue_override_preparation(selected)
        
        # Queue the override download to happen in background thread
        # (reuse the rotation download mechanism to download override playlists to temp folder)
        logger.info("Queuing override content download to background thread...")
        
        # Get playlist objects for download
        override_playlists = selected_playlist_objs
        
        # Download to the temp override folder instead of normal pending folder
        override_pending_folder = self._override_prep_data.get('override_pending_folder') or os.path.join(
            self.config_manager.get_settings().get('video_folder', 'C:/stream_videos/'),
            'temp_override_pending'
        )
        self._background_download_in_progress = True
        loop = asyncio.get_event_loop()
        
        # Create a wrapper that downloads to override temp folder
        def download_override_content():
            try:
                logger.info(f"Background override download starting: {selected}")
                settings = self.config_manager.get_settings()
                verbose_download = settings.get('yt_dlp_verbose', False)
                
                download_result = self.playlist_manager.download_playlists(
                    override_playlists, override_pending_folder, verbose=verbose_download
                )
                
                if download_result.get('success'):
                    logger.info(f"Override download completed: {selected}")
                    self._override_prep_ready = True
                else:
                    logger.warning("Override download had failures")
                    self.notification_service.notify_background_download_warning()
            except Exception as e:
                logger.error(f"Override download error: {e}")
                self.notification_service.notify_background_download_error(str(e))
            finally:
                self._background_download_in_progress = False
        
        loop.run_in_executor(self.executor, download_override_content)
        
        # Don't block - return immediately, commit will happen in future main loop iteration
        # when background download completes AND rotation downloads are idle
        return False  # Return False because we haven't completed the override yet

    async def _auto_resume_pending_downloads(self, session_id: int, pending_playlists: list, status_dict: dict) -> None:
        """Auto-resume interrupted playlist downloads on startup.
        
        When a session resumes with PENDING playlists, automatically trigger their
        downloads immediately instead of waiting for the next rotation trigger.
        Uses yt-dlp's built-in --continue flag to resume from partial downloads.
        
        Args:
            session_id: Database session ID
            pending_playlists: List of playlist names with PENDING status
            status_dict: Dictionary mapping playlist names to their status
        """
        try:
            settings = self.config_manager.get_settings()
            next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
            
            # Ensure folder exists
            os.makedirs(next_folder, exist_ok=True)
            
            # Get playlist objects from database with IDs
            playlist_objects = self.db.get_playlists_with_ids_by_names(pending_playlists)
            if not playlist_objects:
                logger.warning(f"Could not fetch playlist objects for auto-resume: {pending_playlists}")
                return
            
            logger.info(f"Auto-resuming {len(playlist_objects)} interrupted playlist downloads on startup")
            
            # Trigger downloads in background thread (non-blocking)
            def resume_downloads():
                try:
                    # Download playlists with --continue flag (enabled by default in yt-dlp)
                    # and --write-info-json for metadata-aware resumption
                    verbose_download = settings.get('yt_dlp_verbose', False)
                    result = self.playlist_manager.download_playlists(playlist_objects, next_folder, verbose=verbose_download)
                    
                    if result.get('success'):
                        logger.info(f"Auto-resumed downloads completed for: {pending_playlists}")
                        # Update status for completed playlists
                        for playlist in pending_playlists:
                            self.db.update_playlist_status(session_id, playlist, "COMPLETED")
                        # Signal that background downloads are complete
                        self._background_download_in_progress = False
                    else:
                        logger.warning(f"Auto-resumed downloads had failures for: {pending_playlists}")
                        self.notification_service.notify_background_download_warning()
                        # Still mark as complete even if there were failures, so we don't get stuck
                        self._background_download_in_progress = False
                except Exception as e:
                    logger.error(f"Error during auto-resume of downloads: {e}")
                    self.notification_service.notify_background_download_error(str(e))
                    # Still mark as complete on error so we don't get stuck
                    self._background_download_in_progress = False
            
            # Run in background thread
            loop = asyncio.get_event_loop()
            loop.run_in_executor(self.executor, resume_downloads)
            logger.info("Auto-resume background task started")
            
        except Exception as e:
            logger.error(f"Failed to initiate auto-resume of pending downloads: {e}")

    async def run(self):
        """Main automation loop."""
        logger.info("Starting 24/7 Stream Automation")

        # Connect and initialize
        if not self.connect_obs():
            logger.error("Cannot start without OBS connection")
            return

        if not self.obs_controller:
            logger.error("OBS controller not initialized")
            return
        
        if not self.obs_controller.verify_scenes([SCENE_LIVE, SCENE_OFFLINE, SCENE_CONTENT_SWITCH]):
            logger.error("Missing required OBS scenes")
            return

        self.setup_platforms()
        self._initialize_handlers()

        # Sync and check for startup conditions
        config_playlists = self.config_manager.get_playlists()
        self.db.sync_playlists_from_config(config_playlists)

        logger.info("Checking for manual override on startup...")
        override_triggered = await self.check_manual_override()
        
        if not override_triggered:
            session = self.db.get_current_session()
            settings = self.config_manager.get_settings()
            video_folder = settings.get('video_folder', 'C:/stream_videos/')

            if not session:
                logger.info("No active session, starting initial rotation")
                if self.start_rotation_session():
                    await self.execute_content_switch()
            elif not os.path.exists(video_folder) or not os.listdir(video_folder):
                logger.warning(f"Video folder empty/missing: {video_folder}")
                if session.get('id'):
                    self.db.update_session_playback(session['id'], self.playback_tracker.get_total_seconds())
                    self.db.end_session(session['id'])
                if self.start_rotation_session():
                    await self.execute_content_switch()
            else:
                self.current_session_id = session['id']
                # Keep temp playback handler in sync
                if self.temp_playback_handler:
                    self.temp_playback_handler.set_session_id(self.current_session_id)
                playback_seconds = session.get('playback_seconds', 0)
                self.playback_tracker.total_playback_seconds = playback_seconds
                logger.info(f"Resuming session {self.current_session_id}, playback: {playback_seconds}s")
                
                # Check for temp playback state that needs to be restored (crash recovery)
                temp_state = self.db.get_temp_playback_state(session['id'])
                temp_playback_restored = False
                if temp_state and temp_state.get('active') and self.temp_playback_handler:
                    logger.info("Detected interrupted temp playback session, attempting recovery...")
                    restored = await self.temp_playback_handler.restore(session, temp_state)
                    if restored:
                        logger.info("Successfully restored temp playback state")
                        temp_playback_restored = True
                        # Skip normal session resume - temp playback handles its own state
                    else:
                        logger.warning("Failed to restore temp playback, continuing with normal session resume")
                        # Clear the invalid temp playback state
                        self.db.clear_temp_playback_state(session['id'])
                
                # Skip normal session resume logic if temp playback was restored
                if not temp_playback_restored:
                    # Reset download flag but mark as just resumed to skip initial download trigger
                    self._downloads_triggered_this_rotation = False
                    self._just_resumed_session = True
                    
                    # Restore prepared playlists from database
                    next_playlists = session.get('next_playlists')
                    next_playlists_status = session.get('next_playlists_status')
                    
                    if next_playlists:
                        try:
                            playlist_list = json.loads(next_playlists) if isinstance(next_playlists, str) else next_playlists
                            status_dict = json.loads(next_playlists_status) if isinstance(next_playlists_status, str) else (next_playlists_status or {})
                            
                            # Check if all playlists are COMPLETED
                            all_completed = all(status_dict.get(pl) == "COMPLETED" for pl in playlist_list)
                            
                            if all_completed:
                                # Validate that prepared playlist files actually exist in pending folder
                                next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
                                files_exist = self.db.validate_prepared_playlists_exist(session['id'], next_folder)
                                
                                if files_exist:
                                    # Fetch playlist objects from database with IDs (needed for start_rotation_session)
                                    playlist_objects = self.db.get_playlists_with_ids_by_names(playlist_list)
                                    if playlist_objects:
                                        self.next_prepared_playlists = playlist_objects
                                        logger.info(f"Restored prepared playlists from database: {playlist_list}")
                                    else:
                                        logger.warning(f"Could not fetch playlist objects for: {playlist_list}")
                                else:
                                    # Files don't exist in pending folder, clear prepared playlists
                                    logger.warning(f"Prepared playlist files missing from pending folder, clearing and will download fresh on next rotation: {playlist_list}")
                                    self.db.set_next_playlists(session['id'], [])
                            else:
                                logger.info(f"Prepared playlists not fully downloaded, auto-resuming downloads now: {status_dict}")
                                # Auto-resume interrupted downloads immediately on startup
                                await self._auto_resume_pending_downloads(session['id'], playlist_list, status_dict)
                        except Exception as e:
                            logger.error(f"Failed to restore prepared playlists: {e}")
                    
                    if session.get('stream_title'):
                        assert self.stream_manager is not None, "Stream manager not initialized"
                        await self.stream_manager.update_title(session['stream_title'])
                    
                    if playback_seconds > 0:
                        self._pending_seek_position_ms = int(playback_seconds * 1000)
                        logger.info(f"Scheduled seek to {playback_seconds}s")
                    
                    self._initialize_skip_detector()

        # Main loop
        loop_count = 0
        while True:
            try:
                # DEBUG MODE
                debug_mode = os.getenv('DEBUG_MODE_ENABLED', False)

                # Every 60 seconds: Check stream status
                if loop_count % 60 == 0:
                    if self.twitch_live_checker:
                        try:
                            self.twitch_live_checker.refresh_token_if_needed()
                            twitch = self.platform_manager.get_platform("Twitch")
                            if twitch and self.twitch_live_checker.token:
                                twitch.update_token(self.twitch_live_checker.token)
                        except Exception as e:
                            logger.warning(f"Failed to refresh Twitch token: {e}")

                    is_live = False
                    if self.twitch_live_checker:
                        is_live = self.twitch_live_checker.is_stream_live(
                            os.getenv("TARGET_TWITCH_STREAMER", "zackrawrr")
                        )

                    if debug_mode:
                        is_live=False

                    if is_live and self.last_stream_status != "live":
                        logger.info("Streamer is LIVE — pausing 24/7 stream")
                        self.playback_tracker.pause_tracking()
                        if self.obs_controller:
                            self.obs_controller.switch_scene(SCENE_LIVE)
                        self.last_stream_status = "live"
                        self.notification_service.notify_streamer_live()
                    elif not is_live and self.last_stream_status != "offline":
                        logger.info("Streamer is OFFLINE — resuming 24/7 stream")
                        if self.obs_controller:
                            self.obs_controller.switch_scene(SCENE_OFFLINE)
                        self.last_stream_status = "offline"
                        self._rotation_postpone_logged = False
                        self.playback_tracker.resume_tracking()
                        self.notification_service.notify_streamer_offline()

                # Every iteration: Check everything
                if self.last_stream_status != "live" and self.playback_tracker.is_tracking():
                    self.playback_tracker.pause_tracking()
                    if self.current_session_id:
                        self.playback_tracker.update_session(self.current_session_id)
                    self.playback_tracker.resume_tracking()

                self.auto_save_playback_position()
                self._process_video_registration_queue()
                self._process_pending_database_operations()
                
                # Handle skip detector initialization delay after override restoration
                if self._skip_detector_init_delay > 0:
                    self._skip_detector_init_delay -= 1
                    if self._skip_detector_init_delay == 0:
                        logger.info("Initialization delay complete, initializing skip detector after override restoration")
                        self._initialize_skip_detector()
                
                # Handle rotation check delay after override restoration
                if self._skip_rotation_check_delay > 0:
                    self._skip_rotation_check_delay -= 1
                
                # Monitor temp playback for new files and save cursor position
                if self.temp_playback_handler and self.temp_playback_handler.is_active:
                    current_time = time.time()
                    if current_time - self.temp_playback_handler._last_folder_check >= 2.0:  # Check every 2 seconds
                        await self.temp_playback_handler.monitor()
                        self.temp_playback_handler._last_folder_check = current_time
                        
                        # Save cursor position for crash recovery
                        if self.current_session_id and self.obs_controller:
                            media_status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
                            if media_status:
                                cursor_ms = media_status.get('media_cursor', 0) or 0
                                if cursor_ms > 0:
                                    self.db.update_temp_playback_cursor(self.current_session_id, cursor_ms)
                
                # Only check for rotation if not in skip delay period
                if self._skip_rotation_check_delay == 0:
                    await self.check_for_rotation()
                
                # Handle pending seek
                if self._pending_seek_position_ms > 0:
                    if self._seek_retry_count == 0:
                        self.obs_controller.play_media(VLC_SOURCE_NAME)
                        self._seek_retry_count += 1
                        logger.debug("Triggered play, will seek next iteration")
                    else:
                        if self.obs_controller.seek_media(VLC_SOURCE_NAME, self._pending_seek_position_ms):
                            logger.info(f"Seeked VLC to {self._pending_seek_position_ms/1000:.1f}s")
                            self._pending_seek_position_ms = 0
                            self._seek_retry_count = 0
                        else:
                            self._seek_retry_count += 1
                            if self._seek_retry_count >= 11:
                                logger.warning("Failed to seek VLC after 10 attempts, giving up")
                                self._pending_seek_position_ms = 0
                                self._seek_retry_count = 0

                await self.check_manual_override()

                if self.config_manager.has_config_changed():
                    logger.info("Config changed, syncing...")
                    self.db.sync_playlists_from_config(self.config_manager.get_playlists())

                # Shutdown
                if self.shutdown_event:
                    logger.info("Shutdown event detected, cleaning up...")
                    self.save_playback_on_exit()
                    
                    # Cancel all pending asyncio tasks to prevent orphaned tasks from executing with torn-down state
                    try:
                        pending = asyncio.all_tasks()
                        for task in pending:
                            if not task.done():
                                task.cancel()
                        logger.debug(f"Cancelled {len(pending)} pending asyncio tasks")
                    except Exception as e:
                        logger.debug(f"Error cancelling tasks (non-critical): {e}")
                    
                    try:
                        self.platform_manager.cleanup()
                    except Exception as e:
                        logger.debug(f"Platform cleanup warning (non-critical): {e}")
                    if self.obs_client:
                        self.obs_client.disconnect()
                    self.executor.shutdown(wait=True)
                    logger.info("Thread executor shutdown complete")
                    self.db.close()
                    logger.info("Cleanup complete, exiting...")
                    break

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                self.notification_service.notify_automation_error(str(e))

            loop_count += 1
            time.sleep(1)
