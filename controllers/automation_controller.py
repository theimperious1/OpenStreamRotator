"""Main automation controller — orchestrates all components.

Wires managers, handlers, and services together, runs the 1-second
tick main loop, and makes top-level routing decisions (rotation,
temp playback, live status, config changes, shutdown).
"""
import time
import logging
import os
import signal
import asyncio
import json
from threading import Event
from typing import Optional
from dotenv import load_dotenv
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from core.video_registration_queue import VideoRegistrationQueue
from managers.playlist_manager import PlaylistManager
from managers.stream_manager import StreamManager
from managers.obs_connection_manager import OBSConnectionManager
from managers.download_manager import DownloadManager
from managers.rotation_manager import RotationManager
from managers.platform_manager import PlatformManager
from services.notification_service import NotificationService
from playback.file_lock_monitor import FileLockMonitor
from services.twitch_live_checker import TwitchLiveChecker
from services.kick_live_checker import KickLiveChecker
from handlers.content_switch_handler import ContentSwitchHandler
from handlers.temp_playback_handler import TempPlaybackHandler
from utils.video_processor import kill_all_running_processes as kill_processor_processes
from services.web_dashboard_client import WebDashboardClient
from config.constants import (
    DEFAULT_VIDEO_FOLDER, DEFAULT_NEXT_ROTATION_FOLDER,
    DEFAULT_PAUSE_IMAGE, DEFAULT_ROTATION_IMAGE,
    DEFAULT_SCENE_PAUSE, DEFAULT_SCENE_STREAM,
    DEFAULT_SCENE_ROTATION_SCREEN, DEFAULT_VLC_SOURCE_NAME,
)

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# OBS Configuration
OBS_HOST = os.getenv("OBS_HOST", "127.0.0.1")
OBS_PORT = int(os.getenv("OBS_PORT", 4455))
OBS_PASSWORD = os.getenv("OBS_PASSWORD", "")
SCENE_PAUSE = os.getenv("SCENE_PAUSE", os.getenv("SCENE_LIVE", DEFAULT_SCENE_PAUSE))
SCENE_STREAM = os.getenv("SCENE_STREAM", os.getenv("SCENE_OFFLINE", DEFAULT_SCENE_STREAM))
SCENE_ROTATION_SCREEN = os.getenv("SCENE_ROTATION_SCREEN", DEFAULT_SCENE_ROTATION_SCREEN)
VLC_SOURCE_NAME = os.getenv("VLC_SOURCE_NAME", DEFAULT_VLC_SOURCE_NAME)

# Twitch Configuration (used for live checker)
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "")

# Kick Configuration (used for live checker)
KICK_CLIENT_ID = os.getenv("KICK_CLIENT_ID", "")
KICK_CLIENT_SECRET = os.getenv("KICK_CLIENT_SECRET", "")

# Discord Configuration
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# Web Dashboard Configuration
WEB_DASHBOARD_URL = os.getenv("WEB_DASHBOARD_URL", "")      # e.g. ws://localhost:8000
WEB_DASHBOARD_API_KEY = os.getenv("WEB_DASHBOARD_API_KEY", "")  # from team page


class AutomationController:

    def __init__(self):
        # Core managers
        self.db = DatabaseManager()
        self.config_manager = ConfigManager()
        
        # Thread-safe queue for video registration from background downloads
        self.video_registration_queue = VideoRegistrationQueue()
        
        # Shutdown coordination — threading.Event is thread-safe and can interrupt sleeps
        self._shutdown_event = Event()
        
        self.playlist_manager = PlaylistManager(self.db, self.config_manager, self.video_registration_queue, shutdown_event=self._shutdown_event)

        # OBS connection manager
        self.obs_connection = OBSConnectionManager(
            host=OBS_HOST, port=OBS_PORT, password=OBS_PASSWORD,
            shutdown_event=self._shutdown_event,
        )

        # Services
        self.notification_service = NotificationService(DISCORD_WEBHOOK_URL)
        self.file_lock_monitor: Optional[FileLockMonitor] = None

        # Download manager
        self.download_manager = DownloadManager(
            db=self.db,
            config_manager=self.config_manager,
            playlist_manager=self.playlist_manager,
            notification_service=self.notification_service,
            video_registration_queue=self.video_registration_queue,
            shutdown_event=self._shutdown_event,
        )
        
        # Platforms
        self.platform_manager = PlatformManager()
        self.stream_manager: Optional[StreamManager] = None
        
        # Twitch live checker
        if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
            self.twitch_live_checker = TwitchLiveChecker(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
        else:
            self.twitch_live_checker = None

        # Kick live checker
        if KICK_CLIENT_ID and KICK_CLIENT_SECRET:
            self.kick_live_checker = KickLiveChecker(KICK_CLIENT_ID, KICK_CLIENT_SECRET)
        else:
            self.kick_live_checker = None

        # Handlers (initialized in _initialize_handlers)
        self.content_switch_handler: Optional[ContentSwitchHandler] = None
        self.temp_playback_handler: Optional[TempPlaybackHandler] = None

        # State
        self.current_session_id: Optional[int] = None
        self.next_prepared_playlists = None
        self.last_stream_status = None
        self.is_rotating = False
        self._manual_pause = False  # True when paused via dashboard (prevents auto-resume)
        self._rotation_postpone_logged = False
        self._just_resumed_session = False  # Track if we just resumed to skip initial download trigger
        self._shutdown_requested = False
        self._start_time = time.time()  # For uptime tracking
        
        # Web dashboard client (optional, enabled via env vars)
        self.web_dashboard: Optional[WebDashboardClient] = None
        if WEB_DASHBOARD_URL and WEB_DASHBOARD_API_KEY:
            self.web_dashboard = WebDashboardClient(
                api_key=WEB_DASHBOARD_API_KEY,
                state_provider=self._get_dashboard_state,
                command_handler=self._handle_dashboard_command,
                server_url=WEB_DASHBOARD_URL,
            )
        
        # Deferred seek for crash recovery (applied once VLC is confirmed playing)
        self._pending_seek_ms: Optional[int] = None
        self._pending_seek_video: Optional[str] = None

        # Rotation manager (session lifecycle)
        self.rotation_manager = RotationManager(
            self,
            scene_stream=SCENE_STREAM,
            scene_pause=SCENE_PAUSE,
            scene_rotation_screen=SCENE_ROTATION_SCREEN,
            vlc_source_name=VLC_SOURCE_NAME,
        )

        # Wire download manager callbacks
        self.download_manager.set_callbacks(
            get_current_session_id=lambda: self.current_session_id,
            set_next_prepared_playlists=self._set_next_prepared_playlists,
        )

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def _set_next_prepared_playlists(self, playlists) -> None:
        """Callback for download manager to set prepared playlists."""
        self.next_prepared_playlists = playlists

    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received...")
        self._shutdown_event.set()  # Signal download threads to abort
        kill_processor_processes()
        logger.info("Cleanup complete. Setting shutdown flag...")
        self._shutdown_requested = True

    def _initialize_handlers(self):
        """Initialize all handler objects after OBS and services are ready."""
        assert self.obs_controller is not None, "OBS controller must be initialized before handlers"
        
        self.content_switch_handler = ContentSwitchHandler(
            self.db, self.config_manager, self.playlist_manager,
            self.obs_controller, self.notification_service
        )
        self.stream_manager = StreamManager(self.platform_manager)
        
        # Initialize temp playback handler (needs stream_manager)
        self.temp_playback_handler = TempPlaybackHandler(
            self.db, self.config_manager, self.playlist_manager,
            self.obs_controller, self.stream_manager,
            self.notification_service,
            scene_stream=SCENE_STREAM,
            scene_rotation_screen=SCENE_ROTATION_SCREEN,
            vlc_source_name=VLC_SOURCE_NAME
        )
        # Set up callbacks for coordination
        self.temp_playback_handler.set_callbacks(
            auto_resume_downloads=self.download_manager.auto_resume_pending_downloads,
            get_background_download_in_progress=lambda: self.download_manager.background_download_in_progress,
            set_background_download_in_progress=lambda v: setattr(self.download_manager, 'background_download_in_progress', v),
            trigger_next_rotation=self.download_manager.trigger_next_rotation_async,
            reinitialize_file_lock_monitor=self._initialize_file_lock_monitor,
            update_category_after_switch=self._update_category_for_current_video
        )
        
        logger.info("Handlers initialized successfully")

    def _reinitialize_after_obs_reconnect(self):
        """Re-initialize handlers after OBS reconnect, preserving temp playback state.
        
        When OBS disconnects and reconnects, we need new handler instances that
        reference the fresh OBS controller.  However, if temp playback was active,
        the new TempPlaybackHandler must inherit the active state — otherwise the
        system loses track of temp playback and may corrupt the pending folder.
        """
        # Capture temp playback state before handlers are replaced
        temp_was_active = (
            self.temp_playback_handler is not None
            and self.temp_playback_handler.is_active
        )
        temp_session_id = (
            self.temp_playback_handler.current_session_id
            if temp_was_active and self.temp_playback_handler else None
        )

        # Capture file lock monitor temp mode
        monitor_was_temp = (
            self.file_lock_monitor is not None
            and self.file_lock_monitor._temp_playback_mode
        )

        self._initialize_handlers()

        # Restore temp playback state on the new handler instance
        if temp_was_active and self.temp_playback_handler:
            self.temp_playback_handler._active = True
            if temp_session_id is not None:
                self.temp_playback_handler.set_session_id(temp_session_id)
            logger.info("Preserved temp playback active state across OBS reconnect")

        self._initialize_file_lock_monitor()

        # Restore temp mode on the new file lock monitor
        if monitor_was_temp and self.file_lock_monitor:
            self.file_lock_monitor.set_temp_playback_mode(True)
            logger.info("Preserved file lock monitor temp playback mode across OBS reconnect")

        # Restore playback position — VLC restarts from the beginning after
        # OBS reconnects, so use the deferred-seek mechanism (same as crash
        # recovery) to jump back to where we left off.
        if self.current_session_id:
            session = self.db.get_current_session()
            if session:
                saved_video = session.get('playback_current_video')
                saved_cursor = session.get('playback_cursor_ms', 0)
                if saved_video and saved_cursor and saved_cursor > 0:
                    self._pending_seek_ms = saved_cursor
                    self._pending_seek_video = saved_video
                    logger.info(f"Pending seek after OBS reconnect: {saved_video} at {saved_cursor}ms ({saved_cursor/1000:.1f}s)")

    def _get_dashboard_state(self) -> dict:
        """Build a state snapshot for the web dashboard.

        Includes core status fields plus extended data for playlists,
        settings, queue, and platform connections pages.
        """
        status = "offline"
        if self.last_stream_status == "live":
            status = "paused"
        elif self.obs_controller and self.obs_controller.is_connected:
            status = "online"

        current_video: Optional[str] = None
        current_playlist: Optional[str] = None
        current_category: Optional[dict] = None

        if self.file_lock_monitor:
            current_video = self.file_lock_monitor.current_video_original_name

        session = self.db.get_current_session()
        if session:
            playlists_json = session.get('playlists_selected')
            if playlists_json:
                try:
                    playlist_ids = json.loads(playlists_json) if isinstance(playlists_json, str) else playlists_json
                    names = []
                    for pid in playlist_ids:
                        p = self.db.get_playlist(pid)
                        if p:
                            names.append(p['name'])
                    current_playlist = ", ".join(names) if names else None
                except Exception:
                    pass

        if self.file_lock_monitor:
            current_category = self.file_lock_monitor.get_category_for_current_video()

        # ── Extended data ──

        # Playlists from config (name, url, twitch_category, kick_category, enabled, priority)
        playlists = []
        try:
            for p in self.config_manager.get_playlists():
                playlists.append({
                    "name": p.get("name", ""),
                    "url": p.get("url", ""),
                    "twitch_category": p.get("twitch_category", "") or p.get("category", ""),
                    "kick_category": p.get("kick_category", "") or p.get("category", ""),
                    "enabled": p.get("enabled", True),
                    "priority": p.get("priority", 1),
                })
        except Exception:
            pass

        # Settings (all hot-swappable keys from settings.json)
        settings: dict = {}
        try:
            raw = self.config_manager.get_settings()
            # Only expose dashboard-relevant keys (exclude folder paths)
            for key in (
                "stream_title_template",
                "debug_mode",
                "notify_video_transitions",
                "min_playlists_per_rotation",
                "max_playlists_per_rotation",
                "download_retry_attempts",
                "yt_dlp_use_cookies",
                "yt_dlp_browser_for_cookies",
                "yt_dlp_verbose",
            ):
                if key in raw:
                    settings[key] = raw[key]
        except Exception:
            pass

        # Video queue (files in the current rotation folder)
        queue: list[str] = []
        try:
            if self.file_lock_monitor and self.file_lock_monitor.video_folder:
                queue = self.file_lock_monitor._get_video_files()
        except Exception:
            pass

        # Platform connections
        connections: dict = {}
        try:
            connections["obs"] = bool(self.obs_controller and self.obs_controller.is_connected)
            connections["twitch"] = bool(TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET)
            connections["kick"] = bool(KICK_CLIENT_ID and KICK_CLIENT_SECRET)
            connections["discord_webhook"] = bool(DISCORD_WEBHOOK_URL)
            connections["twitch_enabled"] = bool(os.getenv("ENABLE_TWITCH", "").lower() == "true")
            connections["kick_enabled"] = bool(os.getenv("ENABLE_KICK", "").lower() == "true")
        except Exception:
            pass

        # Download status
        download_active = self.download_manager.background_download_in_progress if self.download_manager else False

        return {
            "status": status,
            "manual_pause": self._manual_pause,
            "current_video": current_video,
            "current_playlist": current_playlist,
            "current_category": current_category,
            "obs_connected": bool(self.obs_controller and self.obs_controller.is_connected),
            "uptime_seconds": int(time.time() - self._start_time),
            "playlists": playlists,
            "settings": settings,
            "queue": queue,
            "connections": connections,
            "download_active": download_active,
        }

    async def _handle_dashboard_command(self, command: dict) -> None:
        """Handle a command received from the web dashboard."""
        action = command.get("action", "")
        payload = command.get("payload", {})

        if action == "skip_video":
            logger.info("Dashboard command: skip video")
            if self.obs_controller:
                try:
                    self.obs_controller.obs_client.trigger_media_input_action(
                        name=VLC_SOURCE_NAME,
                        action="OBS_WEBSOCKET_MEDIA_INPUT_ACTION_NEXT",
                    )
                except Exception as e:
                    logger.warning(f"Failed to skip video via dashboard: {e}")

        elif action == "trigger_rotation":
            logger.info("Dashboard command: trigger rotation")
            if not self.is_rotating:
                # Force all-content-consumed so rotation triggers on next tick
                if self.file_lock_monitor:
                    self.file_lock_monitor._all_content_consumed = True

        elif action == "update_setting":
            key = payload.get("key")
            value = payload.get("value")
            if key:
                logger.info(f"Dashboard command: update setting {key}={value}")
                self._apply_setting_from_dashboard(key, value)

        elif action == "add_playlist":
            logger.info(f"Dashboard command: add playlist {payload.get('name')}")
            self._playlist_add(payload)

        elif action == "update_playlist":
            logger.info(f"Dashboard command: update playlist {payload.get('name')}")
            self._playlist_update(payload)

        elif action == "remove_playlist":
            logger.info(f"Dashboard command: remove playlist {payload.get('name')}")
            self._playlist_remove(payload.get("name", ""))

        elif action == "toggle_playlist":
            name = payload.get("name", "")
            enabled = payload.get("enabled")
            logger.info(f"Dashboard command: toggle playlist {name} -> {enabled}")
            self._playlist_toggle(name, enabled)

        elif action == "pause_stream":
            logger.info("Dashboard command: pause stream")
            self._manual_pause_stream()

        elif action == "resume_stream":
            logger.info("Dashboard command: resume stream")
            self._manual_resume_stream()

        else:
            logger.warning(f"Unknown dashboard command: {action}")

    def _apply_setting_from_dashboard(self, key: str, value) -> None:
        """Write a single setting change to settings.json.

        Only whitelisted keys are accepted to prevent arbitrary file writes.
        After writing, the ConfigManager's mtime cache will pick up the
        change automatically on the next get_settings() call.
        """
        allowed_keys = {
            "stream_title_template",
            "debug_mode",
            "notify_video_transitions",
            "min_playlists_per_rotation",
            "max_playlists_per_rotation",
            "download_retry_attempts",
            "yt_dlp_use_cookies",
            "yt_dlp_browser_for_cookies",
            "yt_dlp_verbose",
        }

        if key not in allowed_keys:
            logger.warning(f"Dashboard tried to set disallowed key: {key}")
            return

        settings_path = self.config_manager.settings_path
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            data[key] = value
            with open(settings_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Setting '{key}' updated to {value!r} via dashboard")
        except Exception as e:
            logger.error(f"Failed to update setting '{key}': {e}")

    # ── Manual pause / resume ────────────────────────────────────

    def _manual_pause_stream(self) -> None:
        """Manually pause the stream from the dashboard.

        Reuses the same logic as the streamer-is-live pause: saves playback
        position, switches OBS to the pause scene, and sets the manual flag
        so the live checker won't auto-resume.
        """
        if self.last_stream_status == "live" and self._manual_pause:
            logger.info("Stream is already manually paused")
            return

        # Save playback position
        if self.current_session_id and self.file_lock_monitor and self.obs_controller:
            try:
                status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
                if status and status.get('media_cursor') is not None:
                    current_video = self.file_lock_monitor.current_video_original_name
                    self.db.save_playback_position(
                        self.current_session_id,
                        status['media_cursor'],
                        current_video
                    )
                    logger.info(f"Saved playback position before manual pause: {current_video} at {status['media_cursor']}ms")
            except Exception as e:
                logger.debug(f"Failed to save playback position before manual pause: {e}")

        # Switch to pause scene
        if self.obs_controller:
            self.obs_controller.switch_scene(SCENE_PAUSE)
        if self.file_lock_monitor:
            self.file_lock_monitor._pending_transition_file = None

        self.last_stream_status = "live"
        self._manual_pause = True
        logger.info("Stream manually paused via dashboard")

    def _manual_resume_stream(self) -> None:
        """Manually resume the stream from the dashboard.

        Switches OBS back to the stream scene, clears the manual pause flag,
        and queues a deferred seek to restore playback position.
        """
        if self.last_stream_status != "live":
            logger.info("Stream is not paused — nothing to resume")
            return

        # Switch to stream scene
        if self.obs_controller:
            self.obs_controller.switch_scene(SCENE_STREAM)

        # Restore playback position
        if self.current_session_id:
            session = self.db.get_current_session()
            if session:
                saved_video = session.get('playback_current_video')
                saved_cursor = session.get('playback_cursor_ms', 0)
                if saved_video and saved_cursor and saved_cursor > 0:
                    self._pending_seek_ms = saved_cursor
                    self._pending_seek_video = saved_video
                    logger.info(f"Pending seek after manual resume: {saved_video} at {saved_cursor}ms ({saved_cursor/1000:.1f}s)")

        self.last_stream_status = "offline"
        self._manual_pause = False
        self._rotation_postpone_logged = False
        logger.info("Stream manually resumed via dashboard")

    # ── Playlist CRUD from dashboard ──────────────────────────────

    def _load_playlists_raw(self) -> dict:
        """Load the raw playlists.json file."""
        try:
            with open(self.config_manager.config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load playlists.json: {e}")
            return {"playlists": []}

    def _save_playlists_raw(self, data: dict) -> bool:
        """Write the playlists.json file. Returns True on success."""
        try:
            with open(self.config_manager.config_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info("playlists.json updated via dashboard")
            return True
        except Exception as e:
            logger.error(f"Failed to save playlists.json: {e}")
            return False

    def _playlist_add(self, payload: dict) -> None:
        """Add a new playlist from dashboard payload."""
        name = payload.get("name", "").strip()
        url = payload.get("url", "").strip()
        if not name or not url:
            logger.warning("Dashboard add_playlist missing name or url")
            return

        data = self._load_playlists_raw()
        # Prevent duplicates by name
        for p in data.get("playlists", []):
            if p.get("name", "").lower() == name.lower():
                logger.warning(f"Playlist '{name}' already exists — skipping add")
                return

        data.setdefault("playlists", []).append({
            "name": name,
            "url": url,
            "twitch_category": payload.get("twitch_category", "Just Chatting"),
            "kick_category": payload.get("kick_category", ""),
            "enabled": payload.get("enabled", True),
            "priority": payload.get("priority", 1),
        })
        self._save_playlists_raw(data)

    def _playlist_update(self, payload: dict) -> None:
        """Update an existing playlist's fields (matched by name)."""
        name = payload.get("name", "").strip()
        if not name:
            return

        data = self._load_playlists_raw()
        for p in data.get("playlists", []):
            if p.get("name", "").lower() == name.lower():
                if "url" in payload:
                    p["url"] = payload["url"]
                if "twitch_category" in payload:
                    p["twitch_category"] = payload["twitch_category"]
                if "kick_category" in payload:
                    p["kick_category"] = payload["kick_category"]
                if "enabled" in payload:
                    p["enabled"] = payload["enabled"]
                if "priority" in payload:
                    p["priority"] = payload["priority"]
                self._save_playlists_raw(data)
                return
        logger.warning(f"Playlist '{name}' not found for update")

    def _playlist_remove(self, name: str) -> None:
        """Remove a playlist by name."""
        if not name:
            return
        data = self._load_playlists_raw()
        original_len = len(data.get("playlists", []))
        data["playlists"] = [
            p for p in data.get("playlists", [])
            if p.get("name", "").lower() != name.lower()
        ]
        if len(data["playlists"]) < original_len:
            self._save_playlists_raw(data)
        else:
            logger.warning(f"Playlist '{name}' not found for removal")

    def _playlist_toggle(self, name: str, enabled: bool | None) -> None:
        """Toggle a playlist's enabled state."""
        if not name:
            return
        data = self._load_playlists_raw()
        for p in data.get("playlists", []):
            if p.get("name", "").lower() == name.lower():
                p["enabled"] = enabled if enabled is not None else not p.get("enabled", True)
                self._save_playlists_raw(data)
                return
        logger.warning(f"Playlist '{name}' not found for toggle")

    def save_playback_on_exit(self):
        """Save current state when program exits."""
        # Save final playback position for crash recovery
        if self.current_session_id and self.file_lock_monitor and self.obs_controller:
            try:
                status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
                if status and status.get('media_cursor') is not None:
                    current_video = self.file_lock_monitor.current_video_original_name
                    self.db.save_playback_position(
                        self.current_session_id,
                        status['media_cursor'],
                        current_video
                    )
                    logger.info(f"Saved playback position on exit: {current_video} at {status['media_cursor']}ms")
            except Exception as e:
                logger.debug(f"Failed to save playback position on exit: {e}")
        
        # Switch to pause scene on exit
        if self.obs_controller:
            try:
                self.obs_controller.switch_scene(SCENE_PAUSE)
                logger.info("Switched to pause scene on exit")
            except Exception as e:
                logger.debug(f"Failed to switch scene on exit: {e}")

    # ------------------------------------------------------------------
    # OBS convenience properties (delegate to OBSConnectionManager)
    # ------------------------------------------------------------------

    @property
    def obs_controller(self):
        """Shortcut to the live OBSController instance."""
        return self.obs_connection.controller

    @property
    def obs_client(self):
        """Shortcut to the raw OBS WebSocket client."""
        return self.obs_connection.client

    def setup_platforms(self):
        """Initialize enabled streaming platforms."""
        self.platform_manager.setup(self.twitch_live_checker)

    def _initialize_file_lock_monitor(self, video_folder: Optional[str] = None):
        """Initialize file lock monitor for current rotation.
        
        Args:
            video_folder: Path to the video folder to monitor. If None, uses config default.
        """
        if not self.obs_controller:
            return
        
        if video_folder is None:
            settings = self.config_manager.get_settings()
            video_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)
        
        if self.file_lock_monitor is None:
            self.file_lock_monitor = FileLockMonitor(
                self.db, self.obs_controller, VLC_SOURCE_NAME,
                config=self.config_manager, scene_stream=SCENE_STREAM
            )
        else:
            # Update reference after OBS reconnect (new OBSController instance)
            self.file_lock_monitor.obs_controller = self.obs_controller
        
        self.file_lock_monitor.initialize(str(video_folder))

    async def _update_category_for_current_video(self) -> None:
        """Update stream category based on the video currently playing.
        
        Called after file lock monitor (re)initialization to ensure the category
        matches the actual video VLC is playing.
        """
        if not self.file_lock_monitor or not self.stream_manager:
            return
        
        # Ensure any pending video registrations are in DB first
        self.download_manager.process_video_registration_queue()
        
        category = self.file_lock_monitor.get_category_for_current_video()
        if category:
            try:
                await self.stream_manager.update_category(category)
                logger.info(f"Updated category to '{category}' based on current video")
            except Exception as e:
                logger.warning(f"Failed to update category for current video: {e}")

    async def _apply_config_changes_to_stream(self) -> None:
        """Immediately push category and title updates when playlists.json or settings.json change.
        
        Called from the main loop when has_config_changed() fires.
        - Category: re-resolves from fresh config for the current video.
        - Title: regenerates from the fresh template + current session playlists;
          only pushes if the result actually changed.
        """
        if not self.stream_manager:
            return

        # --- Category ---
        await self._update_category_for_current_video()

        # --- Title ---
        try:
            session = self.db.get_current_session()
            if not session or not self.current_session_id:
                return

            playlists_json = session.get('playlists_selected')
            if not playlists_json:
                return

            playlist_ids = json.loads(playlists_json)
            playlist_names = []
            for pid in playlist_ids:
                p = self.db.get_playlist(pid)
                if p:
                    playlist_names.append(p['name'])

            if not playlist_names:
                return

            new_title = self.playlist_manager.generate_stream_title(playlist_names)
            old_title = session.get('stream_title', '')

            if new_title != old_title:
                await self.stream_manager.update_title(new_title)
                self.db.update_session_stream_title(self.current_session_id, new_title)
                logger.info(f"Stream title updated on config change: '{old_title}' -> '{new_title}'")
            else:
                logger.debug("Config changed but stream title unchanged, skipping title update")
        except Exception as e:
            logger.warning(f"Failed to update stream title on config change: {e}")

    async def _handle_temp_playback_vlc_refresh(self) -> None:
        """Refresh VLC source at the natural end of a video during temp playback.
        
        Called when the file lock monitor detects the last video finished while
        in temp playback mode.  New files may have been downloaded since VLC was
        last loaded — this is the safe moment to reload (video just ended, no
        mid-video disruption for viewers).
        
        If new files exist  → refresh VLC, reinitialize monitor, continue playing.
        If no new files     → delete the finished video, mark consumed, let the
                              normal temp-playback-exit flow handle it.
        """
        if not self.file_lock_monitor:
            return
        
        settings = self.config_manager.get_settings()
        pending_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
        
        # Stop VLC to release file locks before deleting the finished video.
        # A scene switch alone doesn't release VLC's grip on the file.
        if self.obs_controller:
            self.obs_controller.switch_scene(SCENE_ROTATION_SCREEN)
            self.obs_controller.stop_vlc_source(VLC_SOURCE_NAME)
            await asyncio.sleep(0.5)
        
        # Delete the finished video now (monitor deferred deletion for us)
        finished_video = self.file_lock_monitor.current_video
        undeletable_file = None
        if finished_video:
            filepath = os.path.join(pending_folder, finished_video)
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                    logger.info(f"Deleted completed temp playback video: {finished_video}")
                except PermissionError:
                    logger.warning(f"Cannot delete {finished_video} - still locked, excluding from refresh")
                    undeletable_file = finished_video
                except Exception as e:
                    logger.error(f"Failed to delete temp playback video {finished_video}: {e}")
                    undeletable_file = finished_video
        
        # Check if new files are available (exclude the undeletable finished video)
        new_files = self.playlist_manager.get_complete_video_files(pending_folder)
        if undeletable_file and undeletable_file in new_files:
            new_files.remove(undeletable_file)
        
        if new_files:
            logger.info(f"Temp playback VLC refresh: {len(new_files)} files available in pending")
            
            if not self.obs_controller:
                logger.error("No OBS controller available for temp playback VLC refresh")
                return
            
            self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, pending_folder)
            
            await asyncio.sleep(0.3)
            self.obs_controller.switch_scene(SCENE_STREAM)
            
            # Reinitialize monitor to track the refreshed file list
            self.file_lock_monitor.clear_vlc_refresh_flag()
            self._initialize_file_lock_monitor(pending_folder)
            if self.file_lock_monitor:
                self.file_lock_monitor.set_temp_playback_mode(True)
            
            # Update category for the new video
            await self._update_category_for_current_video()
            
            logger.info("Temp playback VLC refreshed — continuing playback")
        else:
            # No new files — mark consumed so normal exit flow kicks in
            logger.info("No new files in pending after last temp video — marking consumed")
            self.file_lock_monitor.clear_vlc_refresh_flag()
            self.file_lock_monitor._all_content_consumed = True

    async def check_for_rotation(self):
        """Check if rotation is needed and handle it."""
        if self.is_rotating:
            return

        session = self.db.get_current_session()
        if not session:
            return

        # Check file lock monitor for video transitions
        # Skip when on the pause screen — VLC isn't playing so all files appear unlocked
        if self.file_lock_monitor and self.last_stream_status != "live":
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
                        # Optional video transition notification
                        settings = self.config_manager.get_settings()
                        if settings.get('notify_video_transitions', False):
                            category = self.file_lock_monitor.get_category_for_current_video() if self.file_lock_monitor else None
                            self.notification_service.notify_video_transition(current_video, category)
                    except Exception as e:
                        logger.warning(f"Failed to update category on video transition: {e}")
            
            # Handle VLC refresh during temp playback: last video finished but
            # new files may have been downloaded.  Refresh VLC at this natural
            # transition point (no mid-video disruption) and reinitialize.
            if self.file_lock_monitor.needs_vlc_refresh:
                await self._handle_temp_playback_vlc_refresh()
                return

        # Trigger background download only if pending folder is empty and not already triggered
        # Skip on first loop after resume to avoid downloading when resuming into existing rotation
        # Skip entirely during temp playback — pending folder is in use for playback
        settings = self.config_manager.get_settings()
        pending_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
        temp_active = self.temp_playback_handler is not None and self.temp_playback_handler.is_active
        if (not temp_active and
            not self.download_manager.downloads_triggered_this_rotation and 
            self.playlist_manager.is_folder_empty(pending_folder) and 
            not self.download_manager.background_download_in_progress and
            not self._just_resumed_session):
            self.download_manager.maybe_start_background_download(self.next_prepared_playlists)
        
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
            next_playlists = DatabaseManager.parse_json_field(session.get('next_playlists'), [])
            next_playlists_status: dict = DatabaseManager.parse_json_field(session.get('next_playlists_status'), {})
            
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
                if self.file_lock_monitor:
                    self.file_lock_monitor.set_temp_playback_mode(True)
                
                # Correct the category based on the actual video VLC is playing
                # (activate() guesses from playlist order, but VLC picks alphabetically)
                await self._update_category_for_current_video()
                
                return
        
        # Check if we should rotate (all content consumed + next rotation ready)
        # Skip during temp playback — monitor() -> exit() handles the proper sequencing
        should_rotate = False
        if all_consumed and not temp_active:
            # Only rotate on pending content if no background download is in progress
            pending_content_ready = has_pending_content and not self.download_manager.background_download_in_progress
            should_rotate = self.next_prepared_playlists or pending_content_ready
        
        if should_rotate:
            if self.next_prepared_playlists:
                logger.info("All content consumed and prepared playlists ready - triggering immediate rotation")
            else:
                logger.info("All content consumed and pending content exists - triggering rotation (prepared from previous run)")
            
            await self.rotation_manager.handle_normal_rotation()
            return

    def _check_live_status(self, debug_mode: bool) -> None:
        """Check if the streamer is live and toggle pause/stream scenes accordingly.

        Checks both Twitch and Kick if configured. Either platform being live
        triggers a pause. Skipped entirely when neither TARGET_TWITCH_STREAMER
        nor TARGET_KICK_STREAMER is set.
        """
        target_twitch = os.getenv("TARGET_TWITCH_STREAMER", "").split("#")[0].strip()
        target_kick = os.getenv("TARGET_KICK_STREAMER", "").split("#")[0].strip()

        # Warn once at startup if a target is set but credentials are missing
        if target_twitch and not self.twitch_live_checker:
            if self.last_stream_status is None:
                logger.warning("TARGET_TWITCH_STREAMER is set but TWITCH_CLIENT_ID/TWITCH_CLIENT_SECRET are missing — Twitch live detection disabled")
        if target_kick and not self.kick_live_checker:
            if self.last_stream_status is None:
                logger.warning("TARGET_KICK_STREAMER is set but KICK_CLIENT_ID/KICK_CLIENT_SECRET are missing — Kick live detection disabled")

        if not target_twitch and not target_kick:
            # No live detection configured — ensure we're on the stream scene
            if self.last_stream_status != "offline":
                if self.obs_controller:
                    self.obs_controller.switch_scene(SCENE_STREAM)
                self.last_stream_status = "offline"
            return

        # Refresh tokens for whichever platform(s) are configured
        if target_twitch and self.twitch_live_checker:
            try:
                self.twitch_live_checker.refresh_token_if_needed()
            except Exception as e:
                logger.warning(f"Failed to refresh Twitch app token: {e}")

        if target_kick and self.kick_live_checker:
            try:
                self.kick_live_checker.refresh_token_if_needed()
            except Exception as e:
                logger.warning(f"Failed to refresh Kick app token: {e}")

        # Check live status on each configured platform
        is_live = False
        if target_twitch and self.twitch_live_checker:
            is_live = self.twitch_live_checker.is_stream_live(target_twitch)
        if not is_live and target_kick and self.kick_live_checker:
            is_live = self.kick_live_checker.is_stream_live(target_kick)

        if debug_mode:
            is_live = False

        if is_live and self.last_stream_status != "live":
            logger.info("Streamer is LIVE — pausing 24/7 stream")
            # Save playback position before pausing so we can resume later
            if self.current_session_id and self.file_lock_monitor and self.obs_controller:
                try:
                    status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
                    if status and status.get('media_cursor') is not None:
                        current_video = self.file_lock_monitor.current_video_original_name
                        self.db.save_playback_position(
                            self.current_session_id,
                            status['media_cursor'],
                            current_video
                        )
                        logger.info(f"Saved playback position before pause: {current_video} at {status['media_cursor']}ms")
                except Exception as e:
                    logger.debug(f"Failed to save playback position before pause: {e}")
            if self.obs_controller:
                self.obs_controller.switch_scene(SCENE_PAUSE)
            if self.file_lock_monitor:
                self.file_lock_monitor._pending_transition_file = None
            self.last_stream_status = "live"
            self.notification_service.notify_streamer_live()
        elif not is_live and self.last_stream_status != "offline":
            if self._manual_pause:
                # Manual pause is active — don't auto-resume when streamer goes offline
                logger.debug("Streamer is OFFLINE but manual pause is active — staying paused")
                return
            logger.info("Streamer is OFFLINE — resuming 24/7 stream")
            if self.obs_controller:
                self.obs_controller.switch_scene(SCENE_STREAM)
            # Restore playback position — VLC may have lost its cursor while paused
            if self.current_session_id:
                session = self.db.get_current_session()
                if session:
                    saved_video = session.get('playback_current_video')
                    saved_cursor = session.get('playback_cursor_ms', 0)
                    if saved_video and saved_cursor and saved_cursor > 0:
                        self._pending_seek_ms = saved_cursor
                        self._pending_seek_video = saved_video
                        logger.info(f"Pending seek after unpause: {saved_video} at {saved_cursor}ms ({saved_cursor/1000:.1f}s)")
            self.last_stream_status = "offline"
            self._rotation_postpone_logged = False
            self.notification_service.notify_streamer_offline()

    def _tick_save_playback(self) -> None:
        """Save playback position every tick and apply deferred seek if pending."""
        if not (self.current_session_id and self.file_lock_monitor and self.obs_controller):
            return
        try:
            status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
            if not status or status.get('media_cursor') is None:
                return

            current_video = self.file_lock_monitor.current_video_original_name
            self.db.save_playback_position(
                self.current_session_id,
                status['media_cursor'],
                current_video
            )

            # Apply deferred seek from crash recovery once VLC is playing
            if self._pending_seek_ms is not None and self.obs_controller:
                media_state = status.get('media_state')
                if media_state and 'playing' in str(media_state).lower():
                    if current_video == self._pending_seek_video:
                        self.obs_controller.seek_media(VLC_SOURCE_NAME, self._pending_seek_ms)
                        logger.info(f"Resumed playback: {self._pending_seek_video} at {self._pending_seek_ms}ms ({self._pending_seek_ms/1000:.1f}s)")
                    else:
                        logger.debug("Video changed before seek could apply, skipping")
                    self._pending_seek_ms = None
                    self._pending_seek_video = None
        except Exception:
            pass  # Non-critical, just skip this tick

    async def _shutdown_cleanup(self) -> None:
        """Perform graceful shutdown: save state, disconnect, stop threads."""
        logger.info("Shutdown event detected, cleaning up...")
        self.notification_service.notify_automation_shutdown()
        self.save_playback_on_exit()

        # Disconnect web dashboard client
        if self.web_dashboard:
            await self.web_dashboard.close()

        try:
            self.platform_manager.cleanup()
        except Exception as e:
            logger.debug(f"Platform cleanup warning (non-critical): {e}")

        # Close async platform resources (e.g., aiohttp sessions in Kick API)
        for platform in self.platform_manager.platforms:
            if hasattr(platform, 'async_close'):
                try:
                    await platform.async_close()  # type: ignore[attr-defined]
                except Exception as e:
                    logger.debug(f"Async platform close warning (non-critical): {e}")

        self.obs_connection.disconnect()

        self.download_manager.shutdown()

        logger.info("Thread executor shutdown complete")
        self.db.close()
        logger.info("Cleanup complete, exiting...")

    async def run(self):
        """Main automation loop."""
        logger.info("Starting 24/7 Stream Automation")

        # Connect and initialize
        if not self.obs_connection.connect():
            logger.error("Cannot start without OBS connection")
            return

        if not self.obs_controller:
            logger.error("OBS controller not initialized")
            return
        
        if not self.obs_controller.ensure_scenes(
            scene_stream=SCENE_STREAM,
            scene_pause=SCENE_PAUSE,
            scene_rotation=SCENE_ROTATION_SCREEN,
            vlc_source_name=VLC_SOURCE_NAME,
            video_folder=self.config_manager.video_folder,
            pause_image=DEFAULT_PAUSE_IMAGE,
            rotation_image=DEFAULT_ROTATION_IMAGE,
        ):
            logger.error("Failed to set up required OBS scenes")
            return

        self.setup_platforms()
        self._initialize_handlers()
        self.notification_service.notify_automation_started()

        # Start web dashboard client if configured
        dashboard_task: Optional[asyncio.Task] = None
        if self.web_dashboard:
            dashboard_task = asyncio.create_task(self.web_dashboard.run())
            logger.info("Web dashboard client started")

        # Sync and check for startup conditions
        config_playlists = self.config_manager.get_playlists()
        self.db.sync_playlists_from_config(config_playlists)

        session = self.db.get_current_session()
        settings = self.config_manager.get_settings()
        video_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)

        if not session:
            logger.info("No active session, starting initial rotation")
            if self.rotation_manager.start_session():
                await self.rotation_manager.execute_content_switch()
        elif not os.path.exists(video_folder) or not os.listdir(video_folder):
            logger.warning(f"Video folder empty/missing: {video_folder}")
            # Before starting fresh, check if session has completed prepared
            # playlists sitting in pending/ — rotate to those instead of
            # re-selecting and re-downloading brand-new playlists.
            next_pl = session.get('next_playlists')
            next_pl_status = session.get('next_playlists_status')
            if next_pl and next_pl_status:
                try:
                    playlist_list = DatabaseManager.parse_json_field(next_pl, [])
                    status_dict = DatabaseManager.parse_json_field(next_pl_status, {})
                    all_completed = all(
                        status_dict.get(pl) == "COMPLETED" for pl in playlist_list
                    )
                    if all_completed:
                        next_folder = settings.get(
                            'next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER
                        )
                        if self.db.validate_prepared_playlists_exist(
                            session['id'], next_folder
                        ):
                            objs = self.db.get_playlists_with_ids_by_names(
                                playlist_list
                            )
                            if objs:
                                self.next_prepared_playlists = objs
                                logger.info(
                                    f"Found completed prepared playlists in "
                                    f"pending, will rotate to: {playlist_list}"
                                )
                except Exception as e:
                    logger.warning(f"Failed to check pending playlists: {e}")
            if session.get('id'):
                self.db.end_session(session['id'])
            if self.rotation_manager.start_session():
                await self.rotation_manager.execute_content_switch()
        else:
            await self.rotation_manager.resume_existing_session(session, settings)

        # Main loop
        loop_count = 0
        last_debug_mode = False
        while True:
            try:
                settings = self.config_manager.get_settings()
                debug_mode = settings.get('debug_mode', False)

                debug_mode_changed = (debug_mode != last_debug_mode)
                if debug_mode_changed:
                    logger.info(f"debug_mode changed to {debug_mode}, forcing live status recheck")
                last_debug_mode = debug_mode

                if loop_count % 60 == 0 or debug_mode_changed:
                    self._check_live_status(debug_mode)

                self.download_manager.process_video_registration_queue()
                self.download_manager.process_pending_database_operations()
                self._tick_save_playback()

                # Proactive OBS health check — detect disconnect even though
                # OBSController swallows exceptions internally
                if self.obs_controller and not self.obs_controller.is_connected:
                    logger.warning("OBS connection lost (detected via health check)")
                    self.notification_service.notify_automation_error("OBS disconnected, attempting reconnect...")
                    if self.obs_connection.reconnect():
                        self._reinitialize_after_obs_reconnect()
                        logger.info("OBS reconnected, handlers re-initialized")
                        continue
                    else:
                        logger.error("Failed to reconnect to OBS, shutting down")
                        break

                # Monitor temp playback for download completion
                if self.temp_playback_handler and self.temp_playback_handler.is_active:
                    current_time = time.time()
                    if current_time - self.temp_playback_handler._last_folder_check >= 2.0:
                        await self.temp_playback_handler.monitor()
                        self.temp_playback_handler._last_folder_check = current_time

                await self.check_for_rotation()

                if self.config_manager.has_config_changed():
                    logger.info("Config changed, syncing...")
                    self.db.sync_playlists_from_config(self.config_manager.get_playlists())
                    await self._apply_config_changes_to_stream()

                if self._shutdown_requested:
                    await self._shutdown_cleanup()
                    break

            except Exception as e:
                error_msg = str(e)
                # Detect OBS disconnection and auto-reconnect
                if any(hint in error_msg.lower() for hint in ('websocket', 'connection', 'socket', 'connect')):
                    logger.warning(f"OBS connection lost: {e}")
                    self.notification_service.notify_automation_error(f"OBS disconnected: {error_msg}")
                    if self.obs_connection.reconnect():
                        self._reinitialize_after_obs_reconnect()
                        logger.info("OBS reconnected, handlers re-initialized")
                        continue
                    else:
                        logger.error("Failed to reconnect to OBS, shutting down")
                        break
                logger.error(f"Error in main loop: {e}", exc_info=True)
                self.notification_service.notify_automation_error(error_msg)

            loop_count += 1
            await asyncio.sleep(1)
