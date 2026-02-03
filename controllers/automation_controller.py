import time
import obsws_python as obs
import logging
import os
import shutil
import signal
import asyncio
from typing import Optional
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

# Platform Configuration
ENABLE_TWITCH = os.getenv("ENABLE_TWITCH", "false").lower() == "true"
ENABLE_KICK = os.getenv("ENABLE_KICK", "false").lower() == "true"

# Twitch Configuration
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "")
TWITCH_USER_LOGIN = os.getenv("TWITCH_USER_LOGIN", "")
TWITCH_BROADCASTER_ID = os.getenv("TWITCH_BROADCASTER_ID", "")

# Kick Configuration
KICK_CLIENT_ID = os.getenv("KICK_CLIENT_ID", "")
KICK_CLIENT_SECRET = os.getenv("KICK_CLIENT_SECRET", "")
KICK_CHANNEL_ID = os.getenv("KICK_CHANNEL_ID", "")
KICK_REDIRECT_URI = os.getenv("KICK_REDIRECT_URI", "http://localhost:8080/callback")

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

        # Executor for background downloads
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="download_worker")

        # State
        self.current_session_id = None
        self.next_prepared_playlists = None
        self.last_stream_status = None
        self.is_rotating = False
        self.download_in_progress = False
        self.override_already_cleaned = False
        self._pending_seek_position_ms = 0
        self._seek_retry_count = 0
        self._last_playback_save_time = 0
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
        self._background_download_in_progress = False  # Normal rotation download active
        
        # Temp playback state (for long playlist handling)
        self._temp_playback_active = False  # Temp playback mode enabled
        self._temp_playback_folder: str = ''  # Temp playback folder path (set dynamically from settings)
        self._last_temp_folder_check = 0  # Track when we last checked for new files

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received...")
        kill_processor_processes()
        logger.info("Cleanup complete. Setting shutdown flag...")
        self.shutdown_event = True

    def _is_pending_folder_empty(self) -> bool:
        """Check if the pending folder is empty or doesn't exist."""
        try:
            settings = self.config_manager.get_settings()
            pending_folder = settings.get('next_rotation_folder', 'C:/stream_videos_pending/')
            
            if not os.path.exists(pending_folder):
                return True
            
            # Check if folder is empty (excluding hidden files)
            items = [f for f in os.listdir(pending_folder) if not f.startswith('.')]
            return len(items) == 0
        except Exception as e:
            logger.warning(f"Error checking pending folder: {e}")
            return False  # Conservative: assume not empty if we can't check

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
        logger.info("Handlers initialized successfully")

    def save_playback_on_exit(self):
        """Save current playback position when program exits."""
        if not self.current_session_id:
            logger.debug("No active session, skipping playback save")
            return
        
        try:
            if not self.obs_controller:
                logger.warning("No OBS controller available, skipping playback save")
                return
            
            current_position_ms = self.obs_controller.get_playback_position_ms(VLC_SOURCE_NAME)
            if current_position_ms is None:
                logger.warning("VLC position is None, skipping save")
                return
            
            playback_seconds = current_position_ms / 1000
            self.db.update_session_playback(self.current_session_id, int(playback_seconds))
            logger.info(f"Saved playback position: {playback_seconds:.1f}s")
            
            if self.obs_controller.switch_scene(SCENE_LIVE):
                logger.info("Switched to pause scene on exit")
            
        except Exception as e:
            logger.error(f"Failed to save playback on exit: {e}")

    def auto_save_playback_position(self):
        """Auto-save playback position every second for power loss resilience."""
        if not self.current_session_id or not self.obs_controller:
            return
        
        try:
            current_position_ms = self.obs_controller.get_playback_position_ms(VLC_SOURCE_NAME)
            if current_position_ms is not None:
                playback_seconds = current_position_ms / 1000
                self.db.update_session_playback(self.current_session_id, int(playback_seconds))
                self._last_playback_save_time = time.time()
        except Exception as e:
            logger.debug(f"Auto-save playback failed (non-critical): {e}")

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
        if self.twitch_live_checker:
            try:
                self.twitch_live_checker.refresh_token_if_needed()
                logger.info("Twitch credentials available for live status checking")
            except Exception as e:
                logger.warning(f"Could not initialize Twitch live checker: {e}")
        
        if ENABLE_TWITCH and self.twitch_live_checker:
            try:
                broadcaster_id = TWITCH_BROADCASTER_ID
                if not broadcaster_id and TWITCH_USER_LOGIN:
                    broadcaster_id = self.twitch_live_checker.get_broadcaster_id(TWITCH_USER_LOGIN)

                if broadcaster_id and self.twitch_live_checker.token:
                    self.platform_manager.add_twitch(
                        TWITCH_CLIENT_ID,
                        self.twitch_live_checker.token,
                        broadcaster_id
                    )
                    logger.info(f"Twitch enabled for channel: {TWITCH_USER_LOGIN}")
                else:
                    logger.warning("Twitch broadcaster ID not found")
            except Exception as e:
                logger.error(f"Failed to setup Twitch: {e}")

        if ENABLE_KICK and KICK_CLIENT_ID and KICK_CLIENT_SECRET and KICK_CHANNEL_ID:
            self.platform_manager.add_kick(
                KICK_CLIENT_ID, KICK_CLIENT_SECRET, KICK_CHANNEL_ID, KICK_REDIRECT_URI
            )
            logger.info(f"Kick enabled for channel ID: {KICK_CHANNEL_ID}")

        enabled = self.platform_manager.get_enabled_platforms()
        if enabled:
            logger.info(f"Enabled platforms: {', '.join(enabled)}")
        else:
            logger.warning("No streaming platforms enabled")

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
        
        from datetime import datetime, timedelta
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
                    import json
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
            if not self.content_switch_handler.finalize_switch(
                current_folder, VLC_SOURCE_NAME, target_scene, SCENE_OFFLINE, self.last_stream_status
            ):
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
            # This will handle category updates on the first video transition
            self._initialize_skip_detector()
            
            # Update category for the currently playing first video
            if self.playback_skip_detector:
                self.playback_skip_detector._update_category_for_current_video()
            
            # Update stream title and category
            try:
                session = self.db.get_current_session()
                if session:
                    import json
                    stream_title = session.get('stream_title', '')
                    category = None
                    playlists_selected = session.get('playlists_selected', '')
                    if playlists_selected:
                        playlist_ids = json.loads(playlists_selected)
                        playlists = self.playlist_manager.get_playlists_by_ids(playlist_ids)
                        if playlists:
                            category = playlists[0].get('category') or playlists[0].get('name')
                    
                    await self.stream_manager.update_stream_info(stream_title, category)
                    logger.info(f"Updated stream: title='{stream_title}', category='{category}'")
            except Exception as e:
                logger.warning(f"Failed to update stream metadata: {e}")

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
        
        session = self.db.get_current_session()
        if not session:
            return
        
        from datetime import datetime
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
            
            # Update stream category based on current video
            current_video = skip_info.get("current_video_filename")
            if current_video and self.content_switch_handler and self.stream_manager:
                try:
                    self.content_switch_handler.update_category_by_video(current_video, self.stream_manager)
                except Exception as e:
                    logger.warning(f"Failed to update category on video transition: {e}")

        # Trigger background download only if pending folder is empty and not already triggered
        # Skip on first loop after resume to avoid downloading when resuming into existing rotation
        if (not self._downloads_triggered_this_rotation and 
            self._is_pending_folder_empty() and 
            not self.download_in_progress and
            not self._just_resumed_session):
            
            playlists = self.rotation_handler.trigger_background_download(
                self.next_prepared_playlists, self.download_in_progress
            )
            if playlists:
                self._downloads_triggered_this_rotation = True
                self.download_in_progress = True
                self._background_download_in_progress = True  # Phase 2: Set flag when download starts
                loop = asyncio.get_event_loop()
                loop.run_in_executor(self.executor, self._sync_background_download_next_rotation, playlists)
                logger.debug("Download triggered (pending folder empty)")
        """ else:
            if not self._is_pending_folder_empty():
                logger.debug("Pending folder not empty, deferring downloads")
            if self.download_in_progress:
                logger.debug("Download already in progress")
            elif self._just_resumed_session:
                logger.debug("Just resumed session, skipping initial download trigger")
        """
        
        # Clear the resume flag after first loop iteration
        if self._just_resumed_session:
            self._just_resumed_session = False

        # Check if all content is consumed and prepared playlists are ready for immediate rotation
        # Also trigger rotation if there's pending content even if prepared_playlists flag is empty (covers restart scenario)
        # Also trigger rotation if there's a suspended session waiting to be restored (override completion)
        has_pending_content = not self._is_pending_folder_empty()
        has_suspended_session = self.db.get_suspended_session() is not None
        should_rotate = False
        
        # Check if temp playback should be activated (long playlist being downloaded)
        if (self.playback_skip_detector is not None and 
            self.playback_skip_detector._all_content_consumed and 
            not self._temp_playback_active and
            not has_suspended_session):
            
            # Check if we have prepared playlists downloading but not completed yet
            next_playlists = session.get('next_playlists', [])
            has_prepared = len(next_playlists) > 0
            
            # Get pending folder status
            pending_folder = os.path.join(self.video_storage_path, 'pending')
            pending_complete_files = self.playlist_manager.get_complete_video_files(pending_folder)
            pending_has_files = len(pending_complete_files) > 0
            
            # Check if prepared playlists are still downloading (not completed)
            pending_incomplete = False
            if next_playlists:
                for playlist in next_playlists:
                    playlist_status = self.db.get_playlist_status(session['session_id'], playlist['id'])
                    if playlist_status and playlist_status[1] != 'COMPLETED':  # status = (name, status_str, ...)
                        pending_incomplete = True
                        break
            
            # Trigger temp playback if all conditions met
            if pending_has_files and pending_incomplete:
                logger.info(f"Long playlist detected downloading ({len(pending_complete_files)} files ready in pending) - activating temp playback")
                await self._activate_temp_playback(session)
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
        
        import json
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
        self.playback_tracker.total_playback_seconds = suspended_session.get('playback_seconds', 0)
        
        from datetime import datetime, timedelta
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
            if not self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, current_folder):
                logger.error("Failed to update VLC source after override restore")
            
            # Switch back to appropriate scene based on stream status
            target_scene = SCENE_LIVE if self.last_stream_status == "live" else SCENE_OFFLINE
            self.obs_controller.switch_scene(target_scene)
            logger.info(f"Switched back to {target_scene} scene after override completion")
        
        self.download_in_progress = False
        self._downloads_triggered_this_rotation = False  # Reset flag when resuming original rotation
        
        # Set delays after override restoration:
        # 1. _skip_detector_init_delay: Wait for VLC to reload playlist and seek to resume position
        # 2. _skip_rotation_check_delay: Wait before checking for rotation to prevent premature trigger
        self._skip_detector_init_delay = 5  # 5 iterations of ~0.5s each = ~2.5 seconds
        self._skip_rotation_check_delay = 10  # 10 iterations of ~0.5s each = ~5 seconds (longer buffer)

    async def _activate_temp_playback(self, session: dict) -> None:
        """Activate temporary playback while large playlist downloads complete.
        
        Scenario: Current rotation finished but next large playlist (e.g., 28 videos)
        still downloading. Move completed files to temp_playback folder and stream those
        while pending continues downloading. Once pending complete, merge and resume normal.
        """
        logger.info("===== TEMP PLAYBACK ACTIVATION =====")
        
        # Get video folder from settings
        settings = self.config_manager.get_settings()
        video_folder = settings.get('video_folder', 'C:/stream_videos/')
        self._temp_playback_folder = os.path.join(video_folder, 'temp_playback')
        
        # Switch to content-switch scene for folder operations
        if not self.obs_controller or not self.obs_controller.switch_scene('content-switch'):
            logger.error("Failed to switch to content-switch scene for temp playback setup")
            return
        
        await asyncio.sleep(1.5)  # Wait for scene switch
        
        try:
            # Create temp_playback folder if it doesn't exist
            os.makedirs(self._temp_playback_folder, exist_ok=True)
            
            # Get complete video files from pending folder
            pending_folder = os.path.join(video_folder, 'pending')
            complete_files = self.playlist_manager.get_complete_video_files(pending_folder)
            
            if not complete_files:
                logger.error("No complete files found in pending folder, cannot activate temp playback")
                return
            
            # Move complete files to temp_playback folder
            if not self.playlist_manager.move_files_to_folder(pending_folder, self._temp_playback_folder, complete_files):
                logger.error("Failed to move files to temp_playback folder")
                return
            
            # Update OBS VLC source to stream from temp_playback
            if not self.obs_controller or not self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, self._temp_playback_folder):
                logger.error("Failed to update VLC source to temp_playback folder")
                return
            
            # Switch back to Stream scene to resume streaming
            await asyncio.sleep(0.5)
            if not self.obs_controller or not self.obs_controller.switch_scene('Stream'):
                logger.error("Failed to switch back to Stream scene after temp playback setup")
                return
            
            # Mark temp playback as active
            self._temp_playback_active = True
            self._last_temp_folder_check = time.time()
            
            # Update skip detector to track files in temp_playback folder
            if self.playback_skip_detector:
                self.playback_skip_detector.video_folder = self._temp_playback_folder
                logger.info(f"Updated skip detector to track temp_playback folder")
            
            logger.info(f"Temp playback activated with {len(complete_files)} files")
            logger.info(f"Streaming from: {self._temp_playback_folder}")
            logger.info("Continue monitoring pending folder for new downloads...")
            
        except Exception as e:
            logger.error(f"Error during temp playback activation: {e}")
            # Switch back to Stream scene on error
            try:
                await asyncio.sleep(0.5)
                if self.obs_controller:
                    self.obs_controller.switch_scene('Stream')
            except Exception as scene_error:
                logger.error(f"Failed to recover scene after temp playback error: {scene_error}")

    async def _monitor_temp_playback(self) -> None:
        """Monitor pending folder for new completed files during temp playback.
        
        Periodically check if new files are ready in pending folder and move them
        to temp_playback folder. Once all pending files are downloaded, exit temp
        playback and prepare for normal rotation.
        """
        try:
            settings = self.config_manager.get_settings()
            video_folder = settings.get('video_folder', 'C:/stream_videos/')
            pending_folder = os.path.join(video_folder, 'pending')
            
            # Get complete files currently in pending folder
            new_complete_files = self.playlist_manager.get_complete_video_files(pending_folder)
            
            # Get files already in temp_playback (to identify truly new ones)
            temp_files = []
            if os.path.exists(self._temp_playback_folder):
                temp_files = [f for f in os.listdir(self._temp_playback_folder) 
                             if os.path.isfile(os.path.join(self._temp_playback_folder, f))]
            
            # Find files that aren't already in temp_playback
            new_files = [f for f in new_complete_files if f not in temp_files]
            
            if new_files:
                logger.info(f"Found {len(new_files)} new completed files in pending folder")
                
                # Move new files to temp_playback
                if self.playlist_manager.move_files_to_folder(pending_folder, self._temp_playback_folder, new_files):
                    logger.info(f"Moved {len(new_files)} new files to temp_playback")
                    
                    # Update OBS VLC source to refresh playlist
                    if not self.obs_controller or not self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, self._temp_playback_folder):
                        logger.warning("Failed to refresh VLC source after moving new files")
                else:
                    logger.error("Failed to move new files to temp_playback folder")
            
            # Check if pending folder is now empty (all files downloaded)
            remaining_files = self.playlist_manager.get_complete_video_files(pending_folder)
            pending_empty = len(remaining_files) == 0
            
            # Also check if there are still .part files (incomplete downloads)
            pending_has_incomplete = False
            if os.path.exists(pending_folder):
                for item in os.listdir(pending_folder):
                    if item.endswith('.part'):
                        pending_has_incomplete = True
                        break
            
            if pending_empty and not pending_has_incomplete:
                logger.info("Pending folder download complete, exiting temp playback")
                await self._exit_temp_playback()
                
        except Exception as e:
            logger.error(f"Error monitoring temp playback: {e}")

    async def _exit_temp_playback(self) -> None:
        """Exit temp playback mode and merge temp + pending folders into live.
        
        Switch back to normal rotation playback from temp_playback folder.
        """
        logger.info("===== TEMP PLAYBACK EXIT =====")
        
        try:
            settings = self.config_manager.get_settings()
            video_folder = settings.get('video_folder', 'C:/stream_videos/')
            
            # Switch to content-switch scene for merge operation
            if not self.obs_controller or not self.obs_controller.switch_scene('content-switch'):
                logger.error("Failed to switch to content-switch scene for temp playback exit")
                return
            
            await asyncio.sleep(1.5)
            
            pending_folder = os.path.join(video_folder, 'pending')
            live_folder = os.path.join(video_folder, 'live')
            
            # Get files from both temp and pending folders
            temp_files = []
            pending_files = []
            
            if os.path.exists(self._temp_playback_folder):
                temp_files = [f for f in os.listdir(self._temp_playback_folder)
                             if os.path.isfile(os.path.join(self._temp_playback_folder, f))]
            
            if os.path.exists(pending_folder):
                pending_files = [f for f in os.listdir(pending_folder)
                               if os.path.isfile(os.path.join(pending_folder, f))]
            
            logger.info(f"Merging temp_playback ({len(temp_files)} files) + pending ({len(pending_files)} files) → live")
            
            # Merge temp and pending into live folder
            source_folders = []
            if temp_files:
                source_folders.append(self._temp_playback_folder)
            if pending_files:
                source_folders.append(pending_folder)
            
            if source_folders:
                if not self.playlist_manager.merge_folders_to_destination(source_folders, live_folder):
                    logger.error("Failed to merge folders during temp playback exit")
                    return
            else:
                logger.warning("No files found to merge during temp playback exit")
            
            # Update OBS to stream from live folder
            await asyncio.sleep(0.5)
            if not self.obs_controller or not self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, live_folder):
                logger.error("Failed to update VLC source to live folder")
                return
            
            # Switch back to Stream scene
            await asyncio.sleep(0.5)
            if not self.obs_controller or not self.obs_controller.switch_scene('Stream'):
                logger.error("Failed to switch back to Stream scene after temp playback exit")
                return
            
            # Clean up temp_playback folder
            try:
                if os.path.exists(self._temp_playback_folder):
                    shutil.rmtree(self._temp_playback_folder)
                    logger.info("Cleaned up temp_playback folder")
            except Exception as cleanup_error:
                logger.error(f"Failed to clean up temp_playback folder: {cleanup_error}")
            
            # Clear temp playback state
            self._temp_playback_active = False
            
            # Update skip detector to track files back in live folder
            if self.playback_skip_detector:
                live_folder = os.path.join(video_folder, 'live')
                self.playback_skip_detector.video_folder = live_folder
                logger.info(f"Updated skip detector to track live folder")
            
            logger.info("Temp playback successfully exited, resuming normal rotation cycle")
            
        except Exception as e:
            logger.error(f"Error during temp playback exit: {e}")
            try:
                await asyncio.sleep(0.5)
                if self.obs_controller:
                    self.obs_controller.switch_scene('Stream')
            except Exception as scene_error:
                logger.error(f"Failed to recover scene after temp playback exit error: {scene_error}")

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
                    import json
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
            self.download_in_progress = False
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
                    self.download_in_progress = True  # Still downloading override content
                    
                    # Start override rotation
                    settings = self.config_manager.get_settings()
                    next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
                    selected = override.get('selected_playlists', [])
                    
                    if self.override_handler.start_override_rotation(selected, next_folder):
                        if self.start_rotation_session(manual_playlists=selected):
                            await self.execute_content_switch()
                    
                    self.download_in_progress = False
                    self._override_prep_ready = False
                    self._override_preparation_pending = False
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
            self.override_handler.clear_override()
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
        override_pending_folder = self._override_prep_data.get('override_pending_folder')
        
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
                playback_seconds = session.get('playback_seconds', 0)
                self.playback_tracker.total_playback_seconds = playback_seconds
                logger.info(f"Resuming session {self.current_session_id}, playback: {playback_seconds}s")
                
                # Reset download flag but mark as just resumed to skip initial download trigger
                self._downloads_triggered_this_rotation = False
                self._just_resumed_session = True
                
                # Restore prepared playlists from database
                next_playlists = session.get('next_playlists')
                next_playlists_status = session.get('next_playlists_status')
                
                if next_playlists:
                    import json
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
                            logger.info(f"Prepared playlists not fully downloaded, will download on next trigger: {status_dict}")
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
                
                # Monitor temp playback for new files
                if self._temp_playback_active:
                    current_time = time.time()
                    if current_time - self._last_temp_folder_check >= 2.0:  # Check every 2 seconds
                        await self._monitor_temp_playback()
                        self._last_temp_folder_check = current_time
                
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
