import time
import requests
import obsws_python as obs
import logging
import os
import signal
import json
import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from managers.playlist_manager import PlaylistManager
from controllers.obs_controller import OBSController
from managers.platform_manager import PlatformManager


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

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

CHECK_INTERVAL = 15
ROTATION_HOURS = 12
ROTATION_SECONDS = ROTATION_HOURS * 3600


class AutomationController:
    def __init__(self):
        self.db = DatabaseManager()
        self.config_manager = ConfigManager()
        self.playlist_manager = PlaylistManager(self.db, self.config_manager)

        self.obs_client = None
        self.obs_controller = None
        self.platform_manager = PlatformManager()

        self.twitch_token = None
        self.twitch_token_expiry = 0

        self.current_session_id = None
        self.playback_start_time = None
        self.total_playback_seconds = 0
        self.last_known_playback_position_ms = 0
        self.next_prepared_playlists = None  # Store playlists downloaded in background

        self.last_stream_status = None
        self.is_rotating = False
        self.download_in_progress = False
        self.download_triggered_for_session = False  # Prevent duplicate download triggers
        self.shutdown_event = False

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received. Setting shutdown flag...")
        self.shutdown_event = True

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
        # Initialize Twitch token for live status checking (independent of ENABLE_TWITCH)
        # This allows checking if Asmongold is live even if we're not updating titles on Twitch
        if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
            try:
                self.twitch_token, self.twitch_token_expiry = self.get_twitch_token()
                logger.info("Twitch credentials available for live status checking")
            except Exception as e:
                logger.warning(f"Could not get Twitch token for live checking: {e}")
        
        # Setup Twitch platform (for title updates)
        if ENABLE_TWITCH and TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET and self.twitch_token:
            # Get broadcaster ID if not set
            broadcaster_id = TWITCH_BROADCASTER_ID
            if not broadcaster_id and TWITCH_USER_LOGIN:
                broadcaster_id = self.get_broadcaster_id(self.twitch_token, TWITCH_USER_LOGIN)

            if broadcaster_id:
                twitch = self.platform_manager.add_twitch(
                    TWITCH_CLIENT_ID,
                    self.twitch_token,
                    broadcaster_id
                )
                if twitch:
                    logger.info(f"Twitch enabled for channel: {TWITCH_USER_LOGIN}")
            else:
                logger.warning("Twitch broadcaster ID not found, Twitch title updates disabled")

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

    def get_twitch_token(self) -> tuple[str, float]:
        """Get Twitch App Access Token."""
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials"
        }
        try:
            r = requests.post(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            token = data["access_token"]
            expiry = time.time() + data.get("expires_in", 3600)
            logger.info("Twitch token acquired")
            return token, expiry
        except requests.RequestException as e:
            logger.error(f"Failed to get Twitch token: {e}")
            raise

    def get_broadcaster_id(self, token: str, username: str) -> str:
        """Get broadcaster ID from username."""
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token}"
        }
        url = f"https://api.twitch.tv/helix/users?login={username}"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get("data"):
                broadcaster_id = data["data"][0]["id"]
                logger.info(f"Got broadcaster ID for {username}: {broadcaster_id}")
                return broadcaster_id
            return ""
        except requests.RequestException as e:
            logger.error(f"Failed to get broadcaster ID: {e}")
            return ""

    def is_stream_live(self, token: str, username: str = "zackrawrr") -> bool:
        """Check if a Twitch user is live."""
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token}"
        }
        url = f"https://api.twitch.tv/helix/streams?user_login={username}"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            is_live = bool(data.get("data"))
            logger.debug(f"Checked {username} live status: {is_live}")
            return is_live
        except requests.RequestException as e:
            logger.error(f"Failed to check stream status for {username}: {e}")
            return False

    def send_discord_notification(self, title: str, description: str, color: int = 0x00FF00):
        """Send Discord notification."""
        if not DISCORD_WEBHOOK_URL:
            return

        payload = {
            "embeds": [{
                "title": title,
                "description": description,
                "color": color,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }]
        }
        try:
            r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to send Discord notification: {e}")

    async def update_stream_titles(self, title: str):
        """Update stream title on all enabled platforms."""
        results = await self.platform_manager.update_title_all(title)

        for platform, success in results.items():
            if not success:
                self.send_discord_notification(
                    f"{platform} Title Update Failed",
                    f"Failed to update title on {platform}",
                    color=0xFF0000
                )

    async def update_stream_info(self, title: str, category: Optional[str] = None):
        """Update stream info (title and category) on all enabled platforms."""
        results = await self.platform_manager.update_stream_info_all(title, category)

        for platform, success in results.items():
            if not success:
                self.send_discord_notification(
                    f"{platform} Stream Update Failed",
                    f"Failed to update stream info on {platform}",
                    color=0xFF0000
                )

    def start_rotation_session(self, manual_playlists=None):
        """Start a new rotation session."""
        logger.info("Starting new rotation session...")

        settings = self.config_manager.get_settings()
        next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
        buffer_minutes = settings.get('download_buffer_minutes', 30)

        # Use prepared playlists if available from background download, otherwise select new ones
        if self.next_prepared_playlists:
            playlists = self.next_prepared_playlists
            self.next_prepared_playlists = None  # Clear it
            logger.info(f"Using prepared playlists: {[p['name'] for p in playlists]}")
        else:
            playlists = self.playlist_manager.select_playlists_for_rotation(manual_playlists)
        
        if not playlists:
            logger.error("No playlists selected for rotation")
            self.send_discord_notification(
                "Rotation Error",
                "No playlists available for rotation",
                color=0xFF0000
            )
            return False

        # Download playlists to next folder
        logger.info(f"Downloading {len(playlists)} playlists...")
        self.send_discord_notification(
            "Content Rotation Started",
            f"Downloading: {', '.join([p['name'] for p in playlists])}",
            color=0xFFA500
        )

        download_result = self.playlist_manager.download_playlists(playlists, next_folder)
        total_duration_seconds = download_result.get('total_duration_seconds', 0)

        if not download_result.get('success'):
            logger.error("Failed to download all playlists")
            self.send_discord_notification(
                "Download Warning",
                "Some playlists failed to download, continuing with available content",
                color=0xFF0000
            )

        # Validate downloads
        if not self.playlist_manager.validate_downloads(next_folder):
            logger.error("Download validation failed")
            self.send_discord_notification(
                "Rotation Failed",
                "Downloaded content validation failed",
                color=0xFF0000
            )
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
        
        # Calculate dynamic buffer: 10% of total duration, but never more than 50% of video
        buffer_percentage = 0.10
        max_buffer_percentage = 0.50
        dynamic_buffer_seconds = int(total_duration_seconds * min(buffer_percentage, max_buffer_percentage))
        download_trigger_time = estimated_finish_time - timedelta(seconds=dynamic_buffer_seconds)
        
        logger.info(f"Total rotation duration: {total_duration_seconds}s (~{total_duration_seconds // 60} minutes)")
        logger.info(f"Estimated finish: {estimated_finish_time}")
        logger.info(f"Next download will trigger at: {download_trigger_time}")

        # Create database session with timing info
        playlist_ids = [p['id'] for p in playlists]
        self.current_session_id = self.db.create_rotation_session(
            playlist_ids, 
            stream_title,
            total_duration_seconds=total_duration_seconds,
            estimated_finish_time=estimated_finish_time,
            download_trigger_time=download_trigger_time
        )
        
        # Reset playback tracking for new rotation
        self.playback_start_time = time.time()
        self.total_playback_seconds = 0
        self.download_triggered_for_session = False  # Reset for new session

        logger.info("Rotation session prepared, ready to switch")
        return True

    async def execute_content_switch(self):
        """Execute the content switch operation."""
        if not self.obs_controller:
            logger.error("OBS controller not initialized")
            return False
        
        logger.info("Executing content switch...")
        self.is_rotating = True

        settings = self.config_manager.get_settings()
        current_folder = settings.get('video_folder', 'C:/stream_videos/')
        next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')

        # Switch to content-switch scene
        self.obs_controller.switch_scene(SCENE_CONTENT_SWITCH)

        # Stop VLC source to allow file hotswap
        self.obs_controller.stop_vlc_source(VLC_SOURCE_NAME)

        time.sleep(3)

        # Switch folders
        success = self.playlist_manager.switch_content_folders(current_folder, next_folder)

        if not success:
            logger.error("Failed to switch content folders")
            self.send_discord_notification(
                "Content Switch Failed",
                "Failed to switch video folders",
                color=0xFF0000
            )
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

        # Reset playback tracking
        self.total_playback_seconds = 0
        self.playback_start_time = time.time() if self.last_stream_status != "live" else None
        
        # Initialize last known position to current VLC position to avoid false skip detection
        media_status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
        self.last_known_playback_position_ms = media_status['media_cursor'] if media_status and media_status['media_cursor'] else 0

        self.send_discord_notification(
            "Content Rotated",
            f"New content is now playing\nTitle: {stream_title}",
            color=0x00FF00
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
                self.send_discord_notification(
                    "Next Rotation Ready",
                    f"Downloaded: {', '.join([p['name'] for p in playlists])}",
                    color=0x00FF00
                )
            else:
                logger.warning("Background download had some failures")
                self.send_discord_notification(
                    "Background Download Warning",
                    "Some playlists failed to download in background",
                    color=0xFF9900
                )
        except Exception as e:
            logger.error(f"Error during background download: {e}")
            self.send_discord_notification(
                "Background Download Error",
                f"Failed to download next rotation: {str(e)}",
                color=0xFF0000
            )
        finally:
            self.download_in_progress = False

    def _check_and_handle_playback_skip(self) -> bool:
        """Detect if user has skipped ahead in video playback and recalculate rotation times.
        
        Returns True if skip was detected and times were recalculated.
        """
        # Get current VLC playback position
        if not self.obs_controller:
            return False
        
        media_status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
        if not media_status:
            return False
        
        current_position_ms = media_status['media_cursor']
        total_duration_ms = media_status['media_duration']
        
        if current_position_ms is None or total_duration_ms is None:
            return False
        
        # Check if playback position jumped significantly ahead of expected
        # Expected advance = CHECK_INTERVAL (15s) + margin (10s) = 25s tolerance
        # Only flag as skip if position jumped MORE than 25 seconds
        EXPECTED_ADVANCE_MS = CHECK_INTERVAL * 1000  # 15,000 ms
        SKIP_MARGIN_MS = 10000  # 10 second margin
        SKIP_THRESHOLD_MS = EXPECTED_ADVANCE_MS + SKIP_MARGIN_MS  # 25,000 ms total
        
        position_delta_ms = current_position_ms - self.last_known_playback_position_ms
        
        # If position jumped significantly more than expected (skip detected)
        if position_delta_ms > SKIP_THRESHOLD_MS:
            time_skipped_seconds = position_delta_ms / 1000
            logger.info(
                f"Playback skip detected: jumped {position_delta_ms}ms ahead "
                f"(from {self.last_known_playback_position_ms}ms to {current_position_ms}ms). "
                f"Time skipped: {time_skipped_seconds:.1f}s"
            )
            
            # Calculate remaining playback time based on current position in video
            remaining_ms = total_duration_ms - current_position_ms
            remaining_seconds = remaining_ms / 1000
            
            # New finish time = now + remaining video seconds
            new_finish_time = datetime.now() + timedelta(seconds=remaining_seconds)
            
            # Update session with new times (if session_id is available)
            if self.current_session_id:
                self.db.update_session_times(
                    self.current_session_id,
                    new_finish_time.isoformat(),
                    (new_finish_time - timedelta(minutes=30)).isoformat()  # download_trigger_time
                )
            
            logger.info(
                f"Rotation times recalculated after skip: "
                f"new finish time = {new_finish_time.isoformat()}"
            )
            
            self.send_discord_notification(
                "Playback Skip Detected",
                f"Video position jumped {time_skipped_seconds:.1f}s ahead. "
                f"Rotation finish time recalculated to: {new_finish_time.strftime('%H:%M:%S')}",
                color=0x0099FF
            )
            
            self.last_known_playback_position_ms = current_position_ms
            return True
        
        # Update last known position for next check
        self.last_known_playback_position_ms = current_position_ms
        return False

    async def check_for_rotation(self):
        """Check if it's time to rotate content based on duration and download trigger."""
        if self.is_rotating:
            return

        # Get current session info
        session = self.db.get_current_session()
        if not session:
            return

        # Check for playback skip and recalculate times if needed
        self._check_and_handle_playback_skip()

        # Check if we need to trigger download of next rotation
        if session.get('download_trigger_time') and not self.download_triggered_for_session:
            download_trigger = datetime.fromisoformat(session['download_trigger_time'])
            if datetime.now() >= download_trigger and not self.download_in_progress:
                logger.info("Download trigger time reached, starting background download of next rotation")
                self.download_triggered_for_session = True  # Mark as triggered
                self.download_in_progress = True
                # Start download in background (non-blocking)
                asyncio.create_task(self._background_download_next_rotation())

        # Check if rotation duration has been reached
        if session.get('estimated_finish_time'):
            finish_time = datetime.fromisoformat(session['estimated_finish_time'])
            if datetime.now() >= finish_time:
                logger.info(f"Rotation duration reached: {self.total_playback_seconds}s")
                
                # End current session
                if self.current_session_id:
                    self.db.update_session_playback(self.current_session_id, self.total_playback_seconds)
                    self.db.end_session(self.current_session_id)
                
                # Start new rotation
                if self.start_rotation_session():
                    await self.execute_content_switch()

    async def check_manual_override(self):
        """Check for manual override requests."""
        if self.config_manager.has_override_changed():
            override = self.config_manager.get_active_override()
            if override and override.get('trigger_now', False):
                logger.info("Manual override triggered")

                # Sync config to ensure new playlists are in database
                config_playlists = self.config_manager.get_playlists()
                self.db.sync_playlists_from_config(config_playlists)

                selected = override.get('selected_playlists', [])

                # End current session
                if self.current_session_id:
                    self.db.update_session_playback(self.current_session_id, self.total_playback_seconds)
                    self.db.end_session(self.current_session_id)

                # Start manual rotation
                if self.start_rotation_session(manual_playlists=selected):
                    await self.execute_content_switch()

                # Clear override
                self.config_manager.clear_override()

    def update_playback_time(self):
        """Update playback time tracking."""
        if self.playback_start_time and self.last_stream_status != "live":
            elapsed = time.time() - self.playback_start_time
            self.total_playback_seconds += int(elapsed)
            self.playback_start_time = time.time()

            # Update session in database periodically
            if self.current_session_id:
                self.db.update_session_playback(self.current_session_id, self.total_playback_seconds)

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
                    self.db.update_session_playback(session['id'], self.total_playback_seconds)
                    self.db.end_session(session['id'])
                # Start new rotation
                if self.start_rotation_session():
                    await self.execute_content_switch()
            else:
                self.current_session_id = session['id']
                self.total_playback_seconds = session.get('playback_seconds', 0)
                stream_title = session.get('stream_title')
                logger.info(f"Resuming session {self.current_session_id}, playback: {self.total_playback_seconds}s")
                # Update stream title to match what was previously playing
                if stream_title:
                    await self.update_stream_titles(stream_title)

        # Main loop
        while True:
            try:
                # Refresh Twitch token if needed (for live status checking)
                if self.twitch_token and time.time() >= self.twitch_token_expiry:
                    try:
                        self.twitch_token, self.twitch_token_expiry = self.get_twitch_token()
                        twitch = self.platform_manager.get_platform("Twitch")
                        if twitch:
                            twitch.update_token(self.twitch_token)
                    except Exception as e:
                        logger.warning(f"Failed to refresh Twitch token: {e}")

                # Check Asmongold stream status (if we have Twitch credentials)
                is_live = False
                if self.twitch_token:
                    is_live = self.is_stream_live(self.twitch_token, os.getenv("TARGET_TWITCH_STREAMER", "zackrawrr"))
                    is_live = False  # TEMP DISABLE FOR TESTING

                if is_live and self.last_stream_status != "live":
                    logger.info("Asmongold is LIVE — pausing 24/7 stream")
                    self.update_playback_time()
                    if self.obs_controller:
                        self.obs_controller.switch_scene(SCENE_LIVE)
                    self.last_stream_status = "live"
                    self.playback_start_time = None

                    self.send_discord_notification(
                        "Asmongold is LIVE!",
                        "24/7 stream paused",
                        color=0x9146FF
                    )

                elif not is_live and self.last_stream_status != "offline":
                    logger.info("Asmongold is OFFLINE — resuming 24/7 stream")
                    if self.obs_controller:
                        self.obs_controller.switch_scene(SCENE_OFFLINE)
                    self.last_stream_status = "offline"
                    self.playback_start_time = time.time()

                    self.send_discord_notification(
                        "Asmongold is OFFLINE",
                        "24/7 stream resumed",
                        color=0x00FF00
                    )

                # Update playback time if streaming
                if not is_live:
                    self.update_playback_time()

                # Check for rotation
                await self.check_for_rotation()

                # Check for manual override
                await self.check_manual_override()

                # Check for config changes
                if self.config_manager.has_config_changed():
                    logger.info("Config file changed, syncing...")
                    config_playlists = self.config_manager.get_playlists()
                    self.db.sync_playlists_from_config(config_playlists)

                # Check for shutdown signal
                if self.shutdown_event:
                    logger.info("Shutdown event detected, performing cleanup...")
                    if self.current_session_id:
                        self.db.update_session_playback(self.current_session_id, self.total_playback_seconds)
                        self.db.end_session(self.current_session_id)
                    self.platform_manager.cleanup()
                    if self.obs_client:
                        self.obs_client.disconnect()
                    self.db.close()
                    logger.info("Cleanup complete, exiting...")
                    break

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                self.send_discord_notification(
                    "Automation Error",
                    f"Unexpected error: {str(e)}",
                    color=0xFF0000
                )

            time.sleep(CHECK_INTERVAL)
