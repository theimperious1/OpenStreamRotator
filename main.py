import asyncio
import time
import requests
import obsws_python as obs
import logging
import os
import signal
import sys
from dotenv import load_dotenv
from database import DatabaseManager
from config_manager import ConfigManager
from playlist_manager import PlaylistManager
from obs_controller import OBSController
from stream_managers.platform_manager import PlatformManager

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('automation.log'),
        logging.StreamHandler()
    ]
)
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
KICK_REDIRECT_URI = os.getenv("KICK_REDIRECT_URI", "https://localhost")

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

        self.last_stream_status = None
        self.is_rotating = False

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received. Cleaning up...")
        if self.current_session_id:
            self.db.end_session(self.current_session_id)
        self.platform_manager.cleanup()
        sys.exit(0)

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
        # Setup Twitch
        if ENABLE_TWITCH and TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
            # Get token
            self.twitch_token, self.twitch_token_expiry = self.get_twitch_token()

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
                logger.warning("Twitch broadcaster ID not found, Twitch disabled")

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

    def start_rotation_session(self, manual_playlists=None):
        """Start a new rotation session."""
        logger.info("Starting new rotation session...")

        settings = self.config_manager.get_settings()
        next_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')

        # Select playlists
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

        success = self.playlist_manager.download_playlists(playlists, next_folder)

        if not success:
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

        # Create database session
        playlist_ids = [p['id'] for p in playlists]
        self.current_session_id = self.db.create_rotation_session(playlist_ids, stream_title)

        logger.info("Rotation session prepared, ready to switch")
        return True

    async def execute_content_switch(self):
        """Execute the content switch operation."""
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

        # Get new stream title from current session
        session = self.db.get_current_session()
        if session:
            stream_title = session['stream_title']
            await self.update_stream_titles(stream_title)
            logger.info(f"Updated stream title: {stream_title}")

        # Switch back to appropriate scene
        if self.last_stream_status == "live":
            self.obs_controller.switch_scene(SCENE_LIVE)
        else:
            self.obs_controller.switch_scene(SCENE_OFFLINE)

        # Reset playback tracking
        self.total_playback_seconds = 0
        self.playback_start_time = time.time() if self.last_stream_status != "live" else None

        self.send_discord_notification(
            "Content Rotated",
            f"New content is now playing\nTitle: {stream_title if session else 'Unknown'}",
            color=0x00FF00
        )

        self.is_rotating = False
        logger.info("Content switch completed successfully")
        return True

    def check_for_rotation(self):
        """Check if it's time to rotate content."""
        if self.is_rotating:
            return

        if self.total_playback_seconds >= ROTATION_SECONDS:
            logger.info(f"Rotation threshold reached: {self.total_playback_seconds}s / {ROTATION_SECONDS}s")

            # End current session
            if self.current_session_id:
                self.db.update_session_playback(self.current_session_id, self.total_playback_seconds)
                self.db.end_session(self.current_session_id)

            # Start new rotation
            if self.start_rotation_session():
                self.execute_content_switch()

    async def check_manual_override(self):
        """Check for manual override requests."""
        if self.config_manager.has_override_changed():
            override = self.config_manager.get_active_override()
            if override and override.get('trigger_now', False):
                logger.info("Manual override triggered")

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
            sys.exit(1)

        # Verify required scenes
        required_scenes = [SCENE_LIVE, SCENE_OFFLINE, SCENE_CONTENT_SWITCH]
        if not self.obs_controller.verify_scenes(required_scenes):
            logger.error("Missing required OBS scenes")
            sys.exit(1)

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
            self.current_session_id = session['id']
            self.total_playback_seconds = session.get('playback_seconds', 0)
            logger.info(f"Resuming session {self.current_session_id}, playback: {self.total_playback_seconds}s")

        # Main loop
        while True:
            try:
                # Refresh Twitch token if needed and enabled
                if ENABLE_TWITCH and time.time() >= self.twitch_token_expiry:
                    self.twitch_token, self.twitch_token_expiry = self.get_twitch_token()
                    twitch = self.platform_manager.get_platform("Twitch")
                    if twitch:
                        twitch.update_token(self.twitch_token)

                # Check Asmongold stream status (only if Twitch enabled)
                is_live = False
                if ENABLE_TWITCH:
                    is_live = self.is_stream_live(self.twitch_token, "asmongold")

                if is_live and self.last_stream_status != "live":
                    logger.info("Asmongold is LIVE — pausing 24/7 stream")
                    self.update_playback_time()
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
                self.check_for_rotation()

                # Check for manual override
                await self.check_manual_override()

                # Check for config changes
                if self.config_manager.has_config_changed():
                    logger.info("Config file changed, syncing...")
                    config_playlists = self.config_manager.get_playlists()
                    self.db.sync_playlists_from_config(config_playlists)

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                self.send_discord_notification(
                    "Automation Error",
                    f"Unexpected error: {str(e)}",
                    color=0xFF0000
                )

            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    controller = AutomationController()
    #controller.run()
    asyncio.run(controller.run())