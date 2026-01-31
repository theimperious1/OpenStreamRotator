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
        self.shutdown_event = False

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

            # Update stream metadata
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
                    
                    # Mark playlists as played
                    self.content_switch_handler.mark_playlists_as_played(session.get('id'))
            except Exception as e:
                logger.warning(f"Failed to update stream metadata: {e}")

            # Initialize skip detector
            self._initialize_skip_detector()

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
                self.db, self.obs_controller, VLC_SOURCE_NAME, video_folder
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
        """Synchronous wrapper for executor."""
        try:
            logger.info(f"Downloading next rotation in thread: {[p['name'] for p in playlists]}")
            settings = self.config_manager.get_settings()
            next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
            
            # Check for verbose yt-dlp logging
            verbose_download = settings.get('yt_dlp_verbose', False)
            
            download_result = self.playlist_manager.download_playlists(playlists, next_folder, verbose=verbose_download)
            
            if download_result.get('success'):
                self.next_prepared_playlists = playlists
                logger.info(f"Background download completed: {[p['name'] for p in playlists]}")
                self.notification_service.notify_next_rotation_ready([p['name'] for p in playlists])
            else:
                logger.warning("Background download had failures")
                self.notification_service.notify_background_download_warning()
        except Exception as e:
            logger.error(f"Background download error: {e}")
            self.notification_service.notify_background_download_error(str(e))

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
            # Background download thread has finished, reset the flag
            self.download_in_progress = False

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

        # Trigger background download
        playlists = self.rotation_handler.trigger_background_download(
            self.next_prepared_playlists, self.download_in_progress
        )
        if playlists:
            self.download_in_progress = True
            loop = asyncio.get_event_loop()
            loop.run_in_executor(self.executor, self._sync_background_download_next_rotation, playlists)

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
        self._initialize_skip_detector()

    async def _handle_normal_rotation(self):
        """Handle normal rotation completion."""
        # Don't rotate if stream is live
        if self.last_stream_status == "live":
            if not self._rotation_postpone_logged:
                logger.info("Stream is live, postponing rotation until stream goes offline")
                self._rotation_postpone_logged = True
            return

        if self.current_session_id:
            total_seconds = self.playback_tracker.get_total_seconds()
            self.db.update_session_playback(self.current_session_id, total_seconds)
            self.db.end_session(self.current_session_id)

        if self.start_rotation_session():
            await self.execute_content_switch()

    async def check_manual_override(self) -> bool:
        """Check for and process manual overrides."""
        assert self.override_handler is not None, "Override handler not initialized"
        
        if not self.override_handler.check_override_triggered():
            return False

        override = self.override_handler.get_active_override()
        if not override or not override.get('trigger_now', False):
            return False

        if not self.override_handler.validate_override(override):
            return False

        logger.info("Manual override triggered")
        
        self.override_handler.sync_config_playlists()
        
        selected = override.get('selected_playlists', [])
        settings = self.config_manager.get_settings()
        next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
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
        
        # Backup prepared rotation (only if we're not replacing an override)
        self.override_handler.backup_prepared_rotation(next_folder)

        # Suspend current session (or the original if we're replacing an override)
        if session_to_suspend:
            self.override_handler.suspend_current_session(
                session_to_suspend, self.next_prepared_playlists,
                self.download_in_progress, override
            )

        # Clear prepared and block further background downloads
        self.next_prepared_playlists = None
        self.download_in_progress = True

        # Start override rotation
        if self.override_handler.start_override_rotation(selected, next_folder):
            if self.start_rotation_session(manual_playlists=selected):
                await self.execute_content_switch()

        self.download_in_progress = False
        self.override_handler.clear_override()
        return True

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
