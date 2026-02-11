import time
import obsws_python as obs
import logging
import os
import signal
import asyncio
import json
from typing import Optional
from datetime import datetime
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
from playback.file_lock_monitor import FileLockMonitor, strip_ordering_prefix
from services.twitch_live_checker import TwitchLiveChecker
from handlers.rotation_handler import RotationHandler
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
        self.file_lock_monitor: Optional[FileLockMonitor] = None
        
        # Twitch live checker
        if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
            self.twitch_live_checker = TwitchLiveChecker(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
        else:
            self.twitch_live_checker = None

        # Handlers (initialized in _initialize_handlers)
        self.rotation_handler: Optional[RotationHandler] = None
        self.content_switch_handler: Optional[ContentSwitchHandler] = None
        self.temp_playback_handler: Optional[TempPlaybackHandler] = None

        # Executor for background downloads
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="download_worker")

        # State
        self.current_session_id = None
        self.next_prepared_playlists = None
        self.last_stream_status = None
        self.is_rotating = False
        self._rotation_postpone_logged = False
        self._downloads_triggered_this_rotation = False  # Track if downloads already triggered after rotation
        self._just_resumed_session = False  # Track if we just resumed to skip initial download trigger
        self._background_download_in_progress = False  # Track if a background download is currently in progress
        self.shutdown_event = False
        
        # Background download state (thread-safe communication from background thread)
        self._pending_db_playlists_to_initialize = None  # Playlists to initialize in DB
        self._pending_db_playlists_to_complete = None    # Playlists to mark as COMPLETED in DB

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
            self.notification_service
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
            auto_resume_downloads=self._auto_resume_pending_downloads,
            get_background_download_in_progress=lambda: self._background_download_in_progress,
            set_background_download_in_progress=lambda v: setattr(self, '_background_download_in_progress', v),
            trigger_next_rotation=self._trigger_next_rotation_async,
            reinitialize_file_lock_monitor=self._initialize_file_lock_monitor,
            update_category_after_switch=self._update_category_for_current_video
        )
        
        logger.info("Handlers initialized successfully")

    def save_playback_on_exit(self):
        """Save current state when program exits."""
        # Switch to pause scene on exit
        if self.obs_controller:
            try:
                self.obs_controller.switch_scene(SCENE_LIVE)
                logger.info("Switched to pause scene on exit")
            except Exception as e:
                logger.debug(f"Failed to switch scene on exit: {e}")

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
        
        logger.info(f"Total rotation duration: {total_duration_seconds}s (~{total_duration_seconds // 60} minutes)")

        playlist_ids = [p['id'] for p in playlists]
        self.current_session_id = self.db.create_rotation_session(
            playlist_ids, stream_title,
            total_duration_seconds=total_duration_seconds
        )
        # Keep temp playback handler in sync
        if self.temp_playback_handler:
            self.temp_playback_handler.set_session_id(self.current_session_id)

        logger.info("Rotation session prepared, ready to switch")
        return True

    async def execute_content_switch(self) -> bool:
        """Execute content switch using handler."""
        assert self.content_switch_handler is not None, "Content switch handler not initialized"
        assert self.stream_manager is not None, "Stream manager not initialized"
        
        if not self.obs_controller:
            logger.error("OBS controller not initialized")
            return False

        logger.info(f"Executing content switch")
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

            # Execute folder operations
            if not self.content_switch_handler.execute_switch(
                current_folder, next_folder
            ):
                self.is_rotating = False
                return False

            # Process any queued videos from downloads so they're in database before rename/category lookup
            self._process_video_registration_queue()

            # Rename videos with playlist ordering prefix (01_, 02_, etc.)
            # so alphabetical ordering groups by playlist
            try:
                session = self.db.get_current_session()
                if session:
                    playlists_selected = session.get('playlists_selected', '')
                    if playlists_selected:
                        playlist_ids = json.loads(playlists_selected)
                        playlists = self.playlist_manager.get_playlists_by_ids(playlist_ids)
                        playlist_order = [p['name'] for p in playlists]
                        self.playlist_manager.rename_videos_with_playlist_prefix(current_folder, playlist_order)
            except Exception as e:
                logger.warning(f"Failed to rename videos with prefix: {e}")

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

            # Initialize file lock monitor for this rotation
            self._initialize_file_lock_monitor(current_folder)
            
            # Update stream title and category based on current video
            try:
                session = self.db.get_current_session()
                if session:
                    stream_title = session.get('stream_title', '')
                    
                    # Get category from first video in rotation
                    category = None
                    if self.file_lock_monitor:
                        category = self.file_lock_monitor.get_category_for_current_video()
                    
                    # Fallback: get category from first playlist
                    if not category and self.content_switch_handler:
                        category = self.content_switch_handler.get_initial_rotation_category(
                            current_folder, self.playlist_manager
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
            logger.info("Content switch completed successfully")
            return True

        except Exception as e:
            logger.error(f"Content switch failed: {e}", exc_info=True)
            self.is_rotating = False
            return False

    def _initialize_file_lock_monitor(self, video_folder: Optional[str] = None):
        """Initialize file lock monitor for current rotation.
        
        Args:
            video_folder: Path to the video folder to monitor. If None, uses config default.
        """
        if not self.obs_controller:
            return
        
        if video_folder is None:
            settings = self.config_manager.get_settings()
            video_folder = settings.get('video_folder', 'C:/stream_videos/')
        
        if self.file_lock_monitor is None:
            self.file_lock_monitor = FileLockMonitor(
                self.db, self.obs_controller, VLC_SOURCE_NAME,
                config=self.config_manager
            )
        
        self.file_lock_monitor.initialize(str(video_folder))

    async def _update_category_for_current_video(self) -> None:
        """Update stream category based on the video currently playing.
        
        Called after file lock monitor (re)initialization to ensure the category
        matches the actual video VLC is playing.
        """
        if not self.file_lock_monitor or not self.stream_manager:
            return
        
        # Ensure any pending video registrations are in DB first
        self._process_video_registration_queue()
        
        category = self.file_lock_monitor.get_category_for_current_video()
        if category:
            try:
                await self.stream_manager.update_category(category)
                logger.info(f"Updated category to '{category}' based on current video")
            except Exception as e:
                logger.warning(f"Failed to update category for current video: {e}")

    async def _trigger_next_rotation_async(self) -> None:
        """Trigger next rotation selection and background download.
        
        Called when temp playback exits to immediately prepare the next rotation
        instead of waiting for the current rotation to finish playing.
        """
        try:
            # Select the next 2 playlists for rotation
            # The selector automatically excludes currently playing and preparing playlists
            next_playlists = self.playlist_manager.select_playlists_for_rotation()
            
            if next_playlists:
                logger.info(f"Auto-triggered next rotation selection after temp playback: {[p['name'] for p in next_playlists]}")
                
                # Start background download
                settings = self.config_manager.get_settings()
                next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
                
                self._background_download_in_progress = True
                loop = asyncio.get_event_loop()
                loop.run_in_executor(self.executor, self._sync_background_download_next_rotation, next_playlists)
            else:
                logger.warning("Failed to auto-select next rotation after temp playback")
        except Exception as e:
            logger.error(f"Error triggering next rotation after temp playback exit: {e}")
    
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

        # Check file lock monitor for video transitions
        if self.file_lock_monitor:
            check_result = self.file_lock_monitor.check()
            
            if check_result['transition']:
                # Video transition detected - update stream category
                current_video = check_result.get('current_video')
                previous_video = check_result.get('previous_video')
                
                # Log the completed video in playback_log
                if previous_video:
                    self.db.log_playback(previous_video, session.get('id'))
                
                if current_video and self.content_switch_handler and self.stream_manager:
                    try:
                        await self.content_switch_handler.update_category_for_video_async(
                            current_video, self.stream_manager
                        )
                    except Exception as e:
                        logger.warning(f"Failed to update category on video transition: {e}")

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

        # Check if all content is consumed
        all_consumed = self.file_lock_monitor is not None and self.file_lock_monitor.all_content_consumed
        has_pending_content = not self.playlist_manager.is_folder_empty(pending_folder)
        
        # Check if temp playback should be activated (long playlist being downloaded)
        if (all_consumed and 
            self.temp_playback_handler and not self.temp_playback_handler.is_active):
            
            # Check if we have prepared playlists downloading but not completed yet
            next_playlists_raw = session.get('next_playlists', [])
            next_playlists_status_raw = session.get('next_playlists_status', {})
            
            # Parse JSON if needed
            next_playlists = json.loads(next_playlists_raw) if isinstance(next_playlists_raw, str) else (next_playlists_raw or [])
            next_playlists_status = json.loads(next_playlists_status_raw) if isinstance(next_playlists_status_raw, str) else (next_playlists_status_raw or {})
            
            # Get pending folder status
            pending_complete_files = self.playlist_manager.get_complete_video_files(pending_folder)
            pending_has_files = len(pending_complete_files) > 0
            
            # Check if prepared playlists are still downloading (not completed)
            pending_incomplete = False
            if next_playlists:
                for playlist_name in next_playlists:
                    if next_playlists_status.get(playlist_name) != 'COMPLETED':
                        pending_incomplete = True
                        break
            
            # Activate temp playback if downloads in progress with files ready
            if pending_has_files and pending_incomplete and self.temp_playback_handler:
                logger.info(f"Long playlist detected downloading ({len(pending_complete_files)} files ready in pending) - activating temp playback")
                await self.temp_playback_handler.activate(session)
                # Re-initialize file lock monitor to watch the pending folder
                self._initialize_file_lock_monitor(pending_folder)
                
                # Correct the category based on the actual video VLC is playing
                # (activate() guesses from playlist order, but VLC picks alphabetically)
                await self._update_category_for_current_video()
                
                return
        
        # Check if we should rotate (all content consumed + next rotation ready)
        should_rotate = False
        if all_consumed:
            # Only rotate on pending content if no background download is in progress
            pending_content_ready = has_pending_content and not self._background_download_in_progress
            should_rotate = self.next_prepared_playlists or pending_content_ready
        
        if should_rotate:
            if self.next_prepared_playlists:
                logger.info("All content consumed and prepared playlists ready - triggering immediate rotation")
            else:
                logger.info("All content consumed and pending content exists - triggering rotation (prepared from previous run)")
            
            await self._handle_normal_rotation()
            return

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
                self.db.end_session(session['id'])
            if self.start_rotation_session():
                await self.execute_content_switch()
        else:
            self.current_session_id = session['id']
            # Keep temp playback handler in sync
            if self.temp_playback_handler:
                self.temp_playback_handler.set_session_id(self.current_session_id)
            logger.info(f"Resuming session {self.current_session_id}")
            
            # Check for temp playback state that needs to be restored (crash recovery)
            temp_state = self.db.get_temp_playback_state(session['id'])
            temp_playback_restored = False
            if temp_state and temp_state.get('active') and self.temp_playback_handler:
                logger.info("Detected interrupted temp playback session, attempting recovery...")
                restored = await self.temp_playback_handler.restore(session, temp_state)
                if restored:
                    logger.info("Successfully restored temp playback state")
                    temp_playback_restored = True
                    # Initialize file lock monitor pointing at the pending folder
                    pending_folder = temp_state.get('folder')
                    if pending_folder:
                        self._initialize_file_lock_monitor(pending_folder)
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
                
                self._initialize_file_lock_monitor()

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
                        self.notification_service.notify_streamer_offline()

                self._process_video_registration_queue()
                self._process_pending_database_operations()
                
                # Monitor temp playback for download completion
                if self.temp_playback_handler and self.temp_playback_handler.is_active:
                    current_time = time.time()
                    if current_time - self.temp_playback_handler._last_folder_check >= 2.0:  # Check every 2 seconds
                        await self.temp_playback_handler.monitor()
                        self.temp_playback_handler._last_folder_check = current_time
                
                # Check for rotation (normal behavior)
                await self.check_for_rotation()

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
