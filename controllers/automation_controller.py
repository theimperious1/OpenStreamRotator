import time
import obsws_python as obs
import logging
import os
import signal
import json
import shutil
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from managers.playlist_manager import PlaylistManager
from controllers.obs_controller import OBSController
from managers.platform_manager import PlatformManager
from services.notification_service import NotificationService
from services.playback_tracker import PlaybackTracker
from services.playback_skip_detector import PlaybackSkipDetector
from services.twitch_live_checker import TwitchLiveChecker
from managers.video_downloader import kill_all_running_processes as kill_downloader_processes
from services.video_processor import kill_all_running_processes as kill_processor_processes


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

# Main loop check interval (seconds)
CHECK_INTERVAL = 1


class AutomationController:
    def __init__(self):
        self.db = DatabaseManager()
        self.config_manager = ConfigManager()
        self.playlist_manager = PlaylistManager(self.db, self.config_manager)

        self.obs_client = None
        self.obs_controller = None
        self.platform_manager = PlatformManager()

        # Initialize services
        self.notification_service = NotificationService(os.getenv("DISCORD_WEBHOOK_URL", ""))
        self.playback_tracker = PlaybackTracker(self.db)
        self.playback_skip_detector: Optional[PlaybackSkipDetector] = None
        
        # Twitch live checker
        if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
            self.twitch_live_checker = TwitchLiveChecker(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
        else:
            self.twitch_live_checker = None

        self.current_session_id = None
        self.next_prepared_playlists = None  # Store playlists downloaded in background

        self.last_stream_status = None
        self.is_rotating = False
        self.download_in_progress = False
        self._pending_seek_position_ms = 0  # Used for seeking on PC restart resume
        self._seek_retry_count = 0  # Track retry attempts for seek
        self._last_playback_save_time = 0  # Track last auto-save time (power loss resilience)
        self._rotation_postpone_logged = False  # Track if we've logged postpone message for current live period
        self._rotation_duration_reached_logged = False  # Track if we've logged rotation reached for current session
        self.shutdown_event = False

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received. Killing any running subprocesses...")
        # Kill subprocesses immediately to prevent hanging
        kill_downloader_processes()
        kill_processor_processes()
        logger.info("Subprocesses killed. Setting shutdown flag...")
        self.shutdown_event = True
    
    def save_playback_on_exit(self):
        """Save current playback position when program exits (for PC restart resume)."""
        if not self.current_session_id:
            logger.debug("No active session, skipping playback save")
            return
        
        try:
            # Get current VLC position
            if not self.obs_controller:
                logger.warning("No OBS controller available, skipping playback save")
                return
            
            media_status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
            if not media_status:
                logger.warning("Could not get VLC status for playback save")
                return
            
            current_position_ms = media_status.get('media_cursor', 0)
            if current_position_ms is None:
                logger.warning("VLC position is None, skipping save")
                return
            
            playback_seconds = current_position_ms / 1000
            
            # Save to database
            self.db.update_session_playback(self.current_session_id, int(playback_seconds))
            logger.info(f"Saved playback position for session {self.current_session_id}: {playback_seconds:.1f}s")
            
            # Switch to pause scene
            if self.obs_controller.switch_scene(SCENE_LIVE):
                logger.info("Switched to pause scene on exit")
            
        except Exception as e:
            logger.error(f"Failed to save playback on exit: {e}")

    def auto_save_playback_position(self):
        """Periodically auto-save playback position (every 1 second) for power loss resilience."""
        if not self.current_session_id:
            return
        
        try:
            # Get current VLC position
            if not self.obs_controller:
                return
            
            media_status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
            if not media_status:
                return
            
            current_position_ms = media_status.get('media_cursor', 0)
            if current_position_ms is None:
                return
            
            playback_seconds = current_position_ms / 1000
            
            # Auto-save to database (silent, no logging to avoid spam)
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
        # Initialize Twitch live checker
        if self.twitch_live_checker:
            try:
                self.twitch_live_checker.refresh_token_if_needed()
                logger.info("Twitch credentials available for live status checking")
            except Exception as e:
                logger.warning(f"Could not initialize Twitch live checker: {e}")
        
        # Setup Twitch platform (for title updates)
        if ENABLE_TWITCH and self.twitch_live_checker:
            try:
                # Get broadcaster ID if not set
                broadcaster_id = TWITCH_BROADCASTER_ID
                if not broadcaster_id and TWITCH_USER_LOGIN:
                    broadcaster_id = self.twitch_live_checker.get_broadcaster_id(TWITCH_USER_LOGIN)

                if broadcaster_id and self.twitch_live_checker.token:
                    twitch = self.platform_manager.add_twitch(
                        TWITCH_CLIENT_ID,
                        self.twitch_live_checker.token,
                        broadcaster_id
                    )
                    if twitch:
                        logger.info(f"Twitch enabled for channel: {TWITCH_USER_LOGIN}")
                else:
                    logger.warning("Twitch broadcaster ID not found, Twitch title updates disabled")
            except Exception as e:
                logger.error(f"Failed to setup Twitch platform: {e}")

        # Setup Kick
        if ENABLE_KICK and KICK_CLIENT_ID and KICK_CLIENT_SECRET and KICK_CHANNEL_ID:
            kick = self.platform_manager.add_kick(
                KICK_CLIENT_ID,
                KICK_CLIENT_SECRET,
                KICK_CHANNEL_ID,
                KICK_REDIRECT_URI
            )
            if kick:
                logger.info(f"Kick enabled for channel ID: {KICK_CHANNEL_ID}")

        # Log enabled platforms
        enabled = self.platform_manager.get_enabled_platforms()
        if enabled:
            logger.info(f"Enabled platforms: {', '.join(enabled)}")
        else:
            logger.warning("No streaming platforms enabled. Titles will not be updated.")

    async def update_stream_titles(self, title: str):
        """Update stream title on all enabled platforms."""
        results = await self.platform_manager.update_title_all(title)

        for platform, success in results.items():
            if not success:
                self.notification_service.notify_stream_update_failed(platform)

    async def update_stream_info(self, title: str, category: Optional[str] = None):
        """Update stream info (title and category) on all enabled platforms."""
        results = await self.platform_manager.update_stream_info_all(title, category)

        for platform, success in results.items():
            if not success:
                self.notification_service.notify_stream_info_update_failed(platform)

    def start_rotation_session(self, manual_playlists=None):
        """Start a new rotation session."""
        logger.info("Starting new rotation session...")
        
        # Reset rotation duration log flag for new session
        self._rotation_duration_reached_logged = False

        settings = self.config_manager.get_settings()
        next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')

        # Use prepared playlists if available from background download, otherwise select new ones
        using_prepared = False
        if self.next_prepared_playlists:
            playlists = self.next_prepared_playlists
            self.next_prepared_playlists = None  # Clear it
            using_prepared = True
            logger.info(f"Using prepared playlists: {[p['name'] for p in playlists]}")
        else:
            playlists = self.playlist_manager.select_playlists_for_rotation(manual_playlists)
        
        if not playlists:
            logger.error("No playlists selected for rotation")
            self.notification_service.notify_rotation_error("No playlists available for rotation")
            return False

        # Download playlists only if not already prepared
        if not using_prepared:
            logger.info(f"Downloading {len(playlists)} playlists...")
            self.notification_service.notify_rotation_started([p['name'] for p in playlists])
            download_result = self.playlist_manager.download_playlists(playlists, next_folder)
            total_duration_seconds = download_result.get('total_duration_seconds', 0)

            if not download_result.get('success'):
                logger.error("Failed to download all playlists")
                self.notification_service.notify_download_warning(
                    "Some playlists failed to download, continuing with available content"
                )
        else:
            # Already prepared, just validate and get duration info
            logger.info(f"Using pre-downloaded playlists, skipping download step")
            download_result = {'success': True}
            total_duration_seconds = 0
            
            # Get duration from already-registered videos
            for playlist in playlists:
                playlist_id = playlist.get('id')
                if playlist_id:
                    videos = self.db.get_videos_by_playlist(playlist_id)
                    for video in videos:
                        total_duration_seconds += video.get('duration_seconds', 0)

        # Validate downloads
        if not self.playlist_manager.validate_downloads(next_folder):
            logger.error("Download validation failed")
            self.notification_service.notify_rotation_error("Downloaded content validation failed")
            return False

        # Generate stream title
        playlist_names = [p['name'] for p in playlists]
        stream_title = self.playlist_manager.generate_stream_title(playlist_names)

        # Calculate timing for predictive downloads
        
        current_time = datetime.now()
        # If we don't have duration info yet, use config rotation_hours as fallback
        if total_duration_seconds == 0:
            rotation_hours = settings.get('rotation_hours', 12)
            total_duration_seconds = rotation_hours * 3600
            logger.info(f"No duration info available, using config rotation_hours: {rotation_hours}h")
        
        estimated_finish_time = current_time + timedelta(seconds=total_duration_seconds)
        
        logger.info(f"Total rotation duration: {total_duration_seconds}s (~{total_duration_seconds // 60} minutes)")
        logger.info(f"Estimated finish: {estimated_finish_time}")

        # Create database session with timing info
        playlist_ids = [p['id'] for p in playlists]
        self.current_session_id = self.db.create_rotation_session(
            playlist_ids, 
            stream_title,
            total_duration_seconds=total_duration_seconds,
            estimated_finish_time=estimated_finish_time,
            download_trigger_time=None  # No longer using trigger time, download immediately after switch
        )
        
        # Reset playback tracking for new rotation
        self.playback_start_time = time.time()
        self.total_playback_seconds = 0

        logger.info("Rotation session prepared, ready to switch")
        return True

    async def execute_content_switch(self, is_override_resumption: bool = False):
        """Execute the content switch operation.
        
        Args:
            is_override_resumption: If True, add override content without wiping existing (for continuity).
                                   If False, do normal switch (wipe + move).
        """
        if not self.obs_controller:
            logger.error("OBS controller not initialized")
            return False
        
        logger.info(f"Executing content switch... (override_resumption={is_override_resumption})")
        self.is_rotating = True

        settings = self.config_manager.get_settings()
        current_folder = settings.get('video_folder', 'C:/stream_videos/')
        next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')

        # Switch to content-switch scene
        self.obs_controller.switch_scene(SCENE_CONTENT_SWITCH)

        # Stop VLC source to allow file hotswap
        self.obs_controller.stop_vlc_source(VLC_SOURCE_NAME)

        time.sleep(3)

        # Check if this is an override situation (suspended session exists)
        is_override_switch = False
        backup_folder = None
        suspended_session = self.db.get_suspended_session()
        if suspended_session and not is_override_resumption:
            # This is the content switch for the OVERRIDE (switching FROM original TO override)
            is_override_switch = True
            suspension_data_str = suspended_session.get('suspension_data', '{}')
            try:
                suspension_data = json.loads(suspension_data_str)
                backup_folder = suspension_data.get('backup_folder')
                logger.info(f"Override content switch: backing up {current_folder} to {backup_folder}")
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Failed to parse suspension data: {e}")

        # Switch folders based on scenario
        if is_override_resumption:
            # Add override content without wiping for continuity
            success = self.playlist_manager.add_override_content(current_folder, next_folder)
        elif is_override_switch and backup_folder:
            # Override content switch: BACKUP current content first, then wipe and move override in
            backup_success = self.playlist_manager.backup_current_content(current_folder, backup_folder)
            if not backup_success:
                logger.error("Failed to backup current content for override")
                self.notification_service.notify_rotation_error("Failed to backup content for override")
                self.is_rotating = False
                return False
            
            # Mark backup as successful in suspension data so we know to restore it
            if suspended_session:
                suspension_data['backup_success'] = True
                self.db.update_session_column(
                    suspended_session['id'],
                    'suspension_data',
                    json.dumps(suspension_data)
                )
            
            # Now do normal switch (wipe + move override)
            success = self.playlist_manager.switch_content_folders(current_folder, next_folder)
        else:
            # Normal rotation: wipe and switch
            success = self.playlist_manager.switch_content_folders(current_folder, next_folder)

        if not success:
            logger.error("Failed to switch content folders")
            self.notification_service.notify_rotation_error("Failed to switch video folders")
            self.is_rotating = False
            return False

        # Update VLC source in OBS
        self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, current_folder)

        # Switch back to appropriate scene FIRST (critical for viewers)
        if self.last_stream_status == "live":
            self.obs_controller.switch_scene(SCENE_LIVE)
        else:
            self.obs_controller.switch_scene(SCENE_OFFLINE)

        # Get new stream title and category from current session
        session = self.db.get_current_session()
        stream_title = "Unknown"
        category = None
        
        if session:
            stream_title = session['stream_title']
            
            # Get the current playlists to find the category
            playlists_selected = session.get('playlists_selected', '')
            if playlists_selected:
                try:
                    playlist_ids = json.loads(playlists_selected)
                    playlists = self.playlist_manager.get_playlists_by_ids(playlist_ids)
                    if playlists and len(playlists) > 0:
                        # Use the first playlist's name as the Kick category search term
                        category = playlists[0].get('name')
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Could not parse playlists_selected: {e}")
            
            # Update stream info with title and category (after scene switch, doesn't block)
            try:
                await self.update_stream_info(stream_title, category)
                logger.info(f"Updated stream: title='{stream_title}', category='{category}'")
            except Exception as e:
                logger.warning(f"Failed to update stream info: {e}")

        # Mark playlists as played for rotation tracking
        if session:
            playlists_selected = session.get('playlists_selected', '')
            if playlists_selected:
                try:
                    playlist_ids = json.loads(playlists_selected)
                    logger.info(f"Marking {len(playlist_ids)} playlists as played: {playlist_ids}")
                    for playlist_id in playlist_ids:
                        self.db.update_playlist_played(playlist_id)
                    logger.info("Successfully marked playlists as played")
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Could not mark playlists as played: {e}")
            else:
                logger.warning("No playlists_selected in session, skipping mark-as-played")
        
        # Reset playback tracking
        self.playback_tracker.reset()
        if self.last_stream_status != "live":
            self.playback_tracker.start_tracking()
        
        # Initialize skip detector
        if self.obs_controller:
            settings = self.config_manager.get_settings()
            video_folder = settings.get('video_folder', 'C:/stream_videos/')
            
            if self.playback_skip_detector is None:
                self.playback_skip_detector = PlaybackSkipDetector(
                    self.db, self.obs_controller, VLC_SOURCE_NAME, video_folder
                )
            
            # Get current session to find total rotation duration and original finish time
            current_session = self.db.get_current_session()
            total_duration = 0
            original_finish = None
            resume_position = 0
            if current_session:
                total_duration = current_session.get('total_duration_seconds', 0)
                finish_time_str = current_session.get('estimated_finish_time')
                if finish_time_str:
                    try:
                        original_finish = datetime.fromisoformat(finish_time_str)
                    except (ValueError, TypeError):
                        pass
                # Get playback position if resuming from pause
                playback_seconds = current_session.get('playback_seconds', 0)
                if playback_seconds > 0:
                    resume_position = playback_seconds * 1000  # Convert to milliseconds
            
            self.playback_skip_detector.initialize(
                total_duration_seconds=total_duration,
                original_finish_time=original_finish,
                resume_position_ms=resume_position
            )

        self.is_rotating = False
        logger.info("Content switch completed successfully")
        return True

    async def _background_download_next_rotation(self):
        """Download next rotation in background without interrupting stream."""
        try:
            logger.info("Starting background download of next rotation...")
            settings = self.config_manager.get_settings()
            next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
            
            # Select next playlists
            playlists = self.playlist_manager.select_playlists_for_rotation()
            if not playlists:
                logger.warning("No playlists available for background download")
                return
            
            # Download in background
            download_result = self.playlist_manager.download_playlists(playlists, next_folder)
            
            if download_result.get('success'):
                # Store playlists for next rotation to use
                self.next_prepared_playlists = playlists
                logger.info(f"Background download completed successfully. Prepared: {[p['name'] for p in playlists]}")
                self.notification_service.notify_next_rotation_ready([p['name'] for p in playlists])
            else:
                logger.warning("Background download had some failures")
                self.notification_service.notify_background_download_warning()
        except Exception as e:
            logger.error(f"Error during background download: {e}")
            self.notification_service.notify_background_download_error(str(e))
        finally:
            self.download_in_progress = False

    async def check_for_rotation(self):
        """Check if it's time to rotate content based on duration."""
        if self.is_rotating:
            return

        # Get current session info
        session = self.db.get_current_session()
        if not session:
            return

        # Check for playback skip and recalculate times if needed
        if self.playback_skip_detector:
            skip_detected, skip_info = self.playback_skip_detector.check_for_skip(self.current_session_id)
            if skip_detected and skip_info:
                self.notification_service.notify_playback_skip(
                    skip_info["time_skipped_seconds"],
                    skip_info["new_finish_time_str"]
                )

        # Trigger background download immediately after rotation (no waiting)
        if not self.download_in_progress and self.next_prepared_playlists is None:
            self.download_in_progress = True
            await self._background_download_next_rotation()

        # Check if rotation duration has been reached
        if session.get('estimated_finish_time'):
            finish_time = datetime.fromisoformat(session['estimated_finish_time'])
            if datetime.now() >= finish_time:
                total_seconds = self.playback_tracker.get_total_seconds()
                
                # Only log once per session rotation (prevent spam)
                if not self._rotation_duration_reached_logged:
                    logger.info(f"Rotation duration reached: {total_seconds}s")
                    self._rotation_duration_reached_logged = True
                
                # Check if this is an override rotation (has suspended session to resume)
                suspended_session = self.db.get_suspended_session()
                if suspended_session:
                    logger.info(f"Override completed, resuming suspended session {suspended_session['id']}")
                    
                    # End the override rotation
                    if self.current_session_id:
                        self.db.update_session_playback(self.current_session_id, total_seconds)
                        self.db.end_session(self.current_session_id)
                    
                    # Restore original content from backup
                    settings = self.config_manager.get_settings()
                    current_folder = settings.get('video_folder', 'C:/stream_videos/')
                    
                    # Parse suspension data to get backup folders
                    suspension_data_str = suspended_session.get('suspension_data', '{}')
                    try:
                        suspension_data = json.loads(suspension_data_str)
                        backup_folder = suspension_data.get('backup_folder')
                        pending_backup_folder = suspension_data.get('pending_backup_folder')
                        prepared_playlist_names = suspension_data.get('prepared_playlist_names', [])
                        
                        if backup_folder and suspension_data.get('backup_success'):
                            logger.info(f"Restoring original content from {backup_folder}")
                            
                            # IMPORTANT: Stop VLC FIRST to release file locks before trying to delete/restore files
                            if self.obs_controller:
                                self.obs_controller.switch_scene(SCENE_CONTENT_SWITCH)
                                self.obs_controller.stop_vlc_source(VLC_SOURCE_NAME)
                                # Wait longer to ensure Windows releases all file handles
                                time.sleep(3)  # Give VLC time to release file locks
                            
                            restore_success = self.playlist_manager.restore_content_after_override(
                                current_folder, backup_folder
                            )
                            if not restore_success:
                                logger.error("CRITICAL: Failed to restore original content from override - aborting override completion to prevent further corruption")
                                logger.error("Please manually restore /live folder from temp_backup_override and /pending from temp_pending_backup")
                                # Don't proceed with /pending restoration if /live failed
                                pending_backup_folder = None  # This will skip the /pending restoration below
                        else:
                            logger.warning("No backup folder in suspension data, skipping restore")
                        
                        # Restore prepared next rotation that was saved before override
                        if pending_backup_folder and os.path.exists(pending_backup_folder):
                            logger.info(f"Restoring prepared next rotation from {pending_backup_folder}")
                            settings = self.config_manager.get_settings()
                            next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
                            
                            # Clear any override remnants from /pending, then restore what we saved
                            os.makedirs(next_folder, exist_ok=True)
                            for filename in os.listdir(next_folder):
                                try:
                                    file_path = os.path.join(next_folder, filename)
                                    if os.path.isfile(file_path):
                                        os.unlink(file_path)
                                    elif os.path.isdir(file_path):
                                        shutil.rmtree(file_path)
                                except Exception as e:
                                    logger.error(f"Error clearing {filename} from {next_folder}: {e}")
                            
                            # Restore from backup
                            for filename in os.listdir(pending_backup_folder):
                                src = os.path.join(pending_backup_folder, filename)
                                dst = os.path.join(next_folder, filename)
                                try:
                                    shutil.move(src, dst)
                                    logger.info(f"Restored prepared content: {filename}")
                                except Exception as e:
                                    logger.error(f"Error restoring {filename}: {e}")
                            
                            # Clean up backup folder
                            try:
                                shutil.rmtree(pending_backup_folder)
                                logger.info(f"Cleaned up pending backup folder: {pending_backup_folder}")
                            except Exception as e:
                                logger.error(f"Error cleaning pending backup folder: {e}")
                            
                            # Restore prepared playlists list from suspension_data
                            # Query database to get full playlist objects with names from the saved list
                            if prepared_playlist_names:
                                restored_playlists = []
                                all_playlists = self.db.get_enabled_playlists()
                                for playlist in all_playlists:
                                    if playlist['name'] in prepared_playlist_names:
                                        restored_playlists.append(playlist)
                                if restored_playlists:
                                    self.next_prepared_playlists = restored_playlists
                                    logger.info(f"Restored prepared playlists: {[p['name'] for p in restored_playlists]}")
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.error(f"Failed to parse suspension data: {e}")
                    
                    # Resume the suspended session
                    self.db.resume_session(suspended_session['id'])
                    
                    # Restore playback tracker state
                    self.current_session_id = suspended_session['id']
                    self.playback_tracker.total_playback_seconds = suspended_session.get('playback_seconds', 0)
                    
                    # Recalculate finish time based on remaining duration
                    original_duration = suspended_session.get('total_duration_seconds', 0)
                    elapsed = suspended_session.get('playback_seconds', 0)
                    remaining_seconds = original_duration - elapsed
                    new_finish_time = datetime.now() + timedelta(seconds=remaining_seconds)
                    
                    # Update session with new finish time
                    self.db.update_session_times(
                        suspended_session['id'],
                        new_finish_time.isoformat(),
                        None
                    )
                    
                    logger.info(f"Resumed session {suspended_session['id']}, remaining duration: {remaining_seconds}s")
                    self.notification_service.notify_override_complete(
                        [suspended_session.get('stream_title', 'Unknown')]
                    )
                    
                    # Allow background downloads to resume now that override is done
                    self.download_in_progress = False
                    
                    # Switch scene back to stream (VLC already stopped and restarted during file restoration)
                    if self.obs_controller:
                        self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, current_folder)
                        
                        # Switch back to stream scene
                        if self.last_stream_status == "live":
                            self.obs_controller.switch_scene(SCENE_LIVE)
                        else:
                            self.obs_controller.switch_scene(SCENE_OFFLINE)
                        
                        # Reset playback tracking
                        self.playback_tracker.reset()
                        if self.last_stream_status != "live":
                            self.playback_tracker.start_tracking()
                        
                        # Reinitialize skip detector with remaining duration for resumed session
                        if self.playback_skip_detector:
                            # For resumed sessions, calculate new finish time for skip detector ceiling
                            resumed_finish = datetime.now() + timedelta(seconds=remaining_seconds)
                            self.playback_skip_detector.initialize(
                                total_duration_seconds=remaining_seconds,
                                original_finish_time=resumed_finish
                            )
                        
                        logger.info("Content restored and VLC restarted for resumed session")
                else:
                    # Normal rotation completion (no suspended session)
                    # Only rotate if stream is currently offline
                    if self.last_stream_status == "live":
                        if not self._rotation_postpone_logged:
                            logger.info("Stream is live, postponing rotation until stream goes offline")
                            self._rotation_postpone_logged = True
                        return
                    
                    if self.current_session_id:
                        self.db.update_session_playback(self.current_session_id, total_seconds)
                        self.db.end_session(self.current_session_id)
                    
                    # Start new rotation
                    if self.start_rotation_session():
                        await self.execute_content_switch()

    async def check_manual_override(self) -> bool:
        """Check for manual override requests.
        
        Returns:
            True if override was triggered, False otherwise
        """
        if self.config_manager.has_override_changed():
            override = self.config_manager.get_active_override()
            if override and override.get('trigger_now', False):
                logger.info("Manual override triggered")

                # Sync config to ensure new playlists are in database
                config_playlists = self.config_manager.get_playlists()
                self.db.sync_playlists_from_config(config_playlists)

                selected = override.get('selected_playlists', [])
                settings = self.config_manager.get_settings()
                next_folder = os.path.normpath(settings.get('next_rotation_folder', 'C:/stream_videos_next/'))
                base_path = os.path.dirname(settings.get('video_folder', 'C:/stream_videos/'))
                backup_folder = os.path.normpath(os.path.join(base_path, 'temp_backup_override'))
                pending_backup_folder = os.path.normpath(os.path.join(base_path, 'temp_pending_backup'))

                # Save what's currently in /pending (prepared next rotation) so we don't lose it
                if os.path.exists(next_folder) and os.listdir(next_folder):
                    next_folder = os.path.normpath(next_folder)
                    pending_backup_folder = os.path.normpath(pending_backup_folder)
                    logger.info(f"Saving prepared next rotation from {next_folder} to {pending_backup_folder}")
                    os.makedirs(pending_backup_folder, exist_ok=True)
                    for filename in os.listdir(next_folder):
                        src = os.path.join(next_folder, filename)
                        dst = os.path.join(pending_backup_folder, filename)
                        try:
                            shutil.move(src, dst)
                            logger.info(f"Saved prepared: {filename}")
                        except Exception as e:
                            logger.error(f"Error saving pending content {src}: {e}")
                    logger.info(f"Pending backup complete: {next_folder} → {pending_backup_folder}")

                # Suspend current session (backup happens later during execute_content_switch)
                if self.current_session_id:
                    total_seconds = self.playback_tracker.get_total_seconds()
                    self.db.update_session_playback(self.current_session_id, total_seconds)
                    
                    # Store the prepared playlists list in suspension_data so we can restore it
                    # BUT: only if no background download is in progress (to avoid race condition)
                    # If a download is in progress when override happens, we'll force a fresh download after
                    prepared_playlist_names = []
                    if self.next_prepared_playlists and not self.download_in_progress:
                        prepared_playlist_names = [p['name'] for p in self.next_prepared_playlists]
                        logger.info(f"Saving prepared playlists for restore: {prepared_playlist_names}")
                    elif self.download_in_progress:
                        logger.warning("Override triggered during background download - will force fresh download after override")
                    
                    suspension_data = {
                        "suspended_by_override": True,
                        "override_playlists": selected,
                        "playback_seconds_before_override": total_seconds,
                        "backup_folder": backup_folder,
                        "pending_backup_folder": pending_backup_folder,
                        "prepared_playlist_names": prepared_playlist_names
                    }
                    self.db.suspend_session(self.current_session_id, suspension_data)
                    logger.info(f"Suspended session {self.current_session_id} for override")

                # Clear prepared playlists to force fresh download of manually selected ones
                # Also prevent background download while override is playing
                self.next_prepared_playlists = None
                self.download_in_progress = True
                
                # Start manual rotation (downloads override to /pending)
                if self.start_rotation_session(manual_playlists=selected):
                    # Once override is downloaded and ready, execute the switch
                    # This is where the backup will happen (VLC already stopped)
                    await self.execute_content_switch()
                
                # Allow background download to proceed for next rotation after override starts
                self.download_in_progress = False
                
                # NOTE: download_in_progress was kept True during override setup to prevent
                # interference, but now that content switch is complete, we allow next rotation
                # to be prepared in background

                # Clear override
                self.config_manager.clear_override()
                return True  # Override was triggered
        
        return False  # No override was triggered

    async def run(self):
        """Main loop."""
        logger.info("Starting 24/7 Stream Automation")

        # Connect to OBS
        if not self.connect_obs():
            logger.error("Cannot start without OBS connection")
            return

        # Verify required scenes
        if not self.obs_controller:
            logger.error("OBS controller not initialized")
            return
        
        required_scenes = [SCENE_LIVE, SCENE_OFFLINE, SCENE_CONTENT_SWITCH]
        if not self.obs_controller.verify_scenes(required_scenes):
            logger.error("Missing required OBS scenes")
            return

        # Setup streaming platforms
        self.setup_platforms()

        # Sync config playlists to database
        config_playlists = self.config_manager.get_playlists()
        self.db.sync_playlists_from_config(config_playlists)

        # Check for manual override on startup FIRST (before auto-rotation)
        logger.info("Checking for manual override on startup...")
        override_triggered = await self.check_manual_override()
        
        if not override_triggered:
            # Only start normal rotation if no override was triggered
            # Check if we need initial rotation
            session = self.db.get_current_session()
            if not session:
                logger.info("No active session, starting initial rotation")
                if self.start_rotation_session():
                    await self.execute_content_switch()
            else:
                # Check if video files still exist
                settings = self.config_manager.get_settings()
                video_folder = settings.get('video_folder', 'C:/stream_videos/')
                
                if not os.path.exists(video_folder) or len(os.listdir(video_folder)) == 0:
                    logger.warning(f"Video folder is empty or missing: {video_folder}")
                    logger.info("Starting new rotation since videos are missing")
                    # End the current session since videos are gone
                    if session['id']:
                        total_seconds = self.playback_tracker.get_total_seconds()
                        self.db.update_session_playback(session['id'], total_seconds)
                        self.db.end_session(session['id'])
                    # Start new rotation
                    if self.start_rotation_session():
                        await self.execute_content_switch()
                else:
                    self.current_session_id = session['id']
                    playback_seconds = session.get('playback_seconds', 0)
                    self.playback_tracker.total_playback_seconds = playback_seconds
                    stream_title = session.get('stream_title')
                    logger.info(f"Resuming session {self.current_session_id}, playback: {playback_seconds}s")
                    # Update stream title to match what was previously playing
                    if stream_title:
                        await self.update_stream_titles(stream_title)
                    
                    # If resuming from a paused position (PC restart), seek VLC to that position
                    if playback_seconds > 0:
                        playback_ms = int(playback_seconds * 1000)
                        # Schedule a seek operation to happen once videos are loaded
                        # We'll do this in the main loop after the first check_for_rotation call
                        self._pending_seek_position_ms = playback_ms
                        logger.info(f"Scheduled seek to {playback_seconds}s ({playback_ms}ms) on playback resume")
                    
                    # Initialize skip detector for resumed session
                    if self.obs_controller:
                        settings = self.config_manager.get_settings()
                        video_folder = settings.get('video_folder', 'C:/stream_videos/')
                        
                        if self.playback_skip_detector is None:
                            self.playback_skip_detector = PlaybackSkipDetector(
                                self.db, self.obs_controller, VLC_SOURCE_NAME, video_folder
                            )
                        
                        # Get rotation duration and finish time from session
                        total_duration = session.get('total_duration_seconds', 0)
                        finish_time_str = session.get('estimated_finish_time')
                        original_finish = None
                        if finish_time_str:
                            try:
                                original_finish = datetime.fromisoformat(finish_time_str)
                            except (ValueError, TypeError):
                                pass
                        
                        # Initialize with resume position
                        resume_position_ms = int(playback_seconds * 1000) if playback_seconds > 0 else 0
                        self.playback_skip_detector.initialize(
                            total_duration_seconds=total_duration,
                            original_finish_time=original_finish,
                            resume_position_ms=resume_position_ms
                        )

        # Main loop
        loop_count = 0
        while True:
            try:
                # Every 60 iterations (60 seconds): Refresh Twitch token and check stream status
                if loop_count % 60 == 0:
                    # Refresh Twitch token if needed (for live status checking)
                    if self.twitch_live_checker:
                        try:
                            self.twitch_live_checker.refresh_token_if_needed()
                            twitch = self.platform_manager.get_platform("Twitch")
                            if twitch and self.twitch_live_checker.token:
                                twitch.update_token(self.twitch_live_checker.token)
                        except Exception as e:
                            logger.warning(f"Failed to refresh Twitch token: {e}")

                    # Check Asmongold stream status (if we have Twitch live checker)
                    is_live = False
                    if self.twitch_live_checker:
                        is_live = self.twitch_live_checker.is_stream_live(
                            os.getenv("TARGET_TWITCH_STREAMER", "zackrawrr")
                        )

                    if is_live and self.last_stream_status != "live":
                        logger.info("Asmongold is LIVE — pausing 24/7 stream")
                        self.playback_tracker.pause_tracking()
                        if self.obs_controller:
                            self.obs_controller.switch_scene(SCENE_LIVE)
                        self.last_stream_status = "live"
                        self.notification_service.notify_asmongold_live()

                    elif not is_live and self.last_stream_status != "offline":
                        logger.info("Asmongold is OFFLINE — resuming 24/7 stream")
                        if self.obs_controller:
                            self.obs_controller.switch_scene(SCENE_OFFLINE)
                        self.last_stream_status = "offline"
                        self._rotation_postpone_logged = False  # Reset postpone flag for next live period
                        self.playback_tracker.resume_tracking()
                        self.notification_service.notify_asmongold_offline()


                # Every iteration (every second): Rotation checks, config changes, playback tracking
                # Update playback time if streaming
                if self.last_stream_status != "live" and self.playback_tracker.is_tracking():
                    self.playback_tracker.pause_tracking()
                    if self.current_session_id:
                        self.playback_tracker.update_session(self.current_session_id)
                    self.playback_tracker.resume_tracking()

                # Auto-save playback position every second for power loss resilience
                self.auto_save_playback_position()

                # Check for rotation (fast detection)
                await self.check_for_rotation()
                
                # Handle pending seek (from PC restart resume)
                if self._pending_seek_position_ms > 0:
                    # First attempt: trigger play to ensure media is playing/paused
                    if self._seek_retry_count == 0:
                        # On first attempt, trigger play and skip seek this iteration
                        # to give VLC time to transition state
                        self.obs_controller.play_media(VLC_SOURCE_NAME)
                        self._seek_retry_count += 1
                        logger.debug("Triggered play, will attempt seek on next iteration")
                    else:
                        # On subsequent attempts, try to seek
                        if self.obs_controller.seek_media(VLC_SOURCE_NAME, self._pending_seek_position_ms):
                            logger.info(f"Seeked VLC to {self._pending_seek_position_ms/1000:.1f}s on resume")
                            self._pending_seek_position_ms = 0  # Clear after successful seek
                            self._seek_retry_count = 0  # Reset retry count
                        else:
                            # Retry up to 10 more times (10 seconds total) before giving up
                            self._seek_retry_count += 1
                            if self._seek_retry_count >= 11:  # 1 for play + 10 for seek attempts
                                logger.warning(f"Failed to seek VLC after 10 attempts, giving up")
                                self._pending_seek_position_ms = 0  # Give up
                                self._seek_retry_count = 0
                            else:
                                logger.debug(f"Seek failed (attempt {self._seek_retry_count - 1}/10), will retry next iteration")

                # Check for manual override
                await self.check_manual_override()

                # Check for config changes (fast response)
                if self.config_manager.has_config_changed():
                    logger.info("Config file changed, syncing...")
                    config_playlists = self.config_manager.get_playlists()
                    self.db.sync_playlists_from_config(config_playlists)

                # Check for shutdown signal
                if self.shutdown_event:
                    logger.info("Shutdown event detected, performing cleanup...")
                    # Save playback position for PC restart resume
                    self.save_playback_on_exit()
                    if self.current_session_id:
                        # Only end session if we didn't already save it in save_playback_on_exit
                        # We want to keep the session active if paused for resume
                        # So we just update it without ending it
                        logger.info(f"Session {self.current_session_id} left in paused state for next startup")
                    self.platform_manager.cleanup()
                    if self.obs_client:
                        self.obs_client.disconnect()
                    self.db.close()
                    logger.info("Cleanup complete, exiting...")
                    break

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                self.notification_service.notify_automation_error(str(e))

            loop_count += 1
            time.sleep(1)
