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
from typing import Optional, List
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
from managers.prepared_rotation_manager import PreparedRotationManager
from services.notification_service import NotificationService
from playback.playback_monitor import PlaybackMonitor
from services.twitch_live_checker import TwitchLiveChecker
from services.kick_live_checker import KickLiveChecker
from handlers.content_switch_handler import ContentSwitchHandler
from handlers.dashboard_handler import DashboardHandler
from handlers.temp_playback_handler import TempPlaybackHandler
from utils.video_processor import kill_all_running_processes as kill_processor_processes
from services.web_dashboard_client import WebDashboardClient
from monitors.obs_freeze_monitor import OBSFreezeMonitor
from config.constants import (
    DEFAULT_VIDEO_FOLDER, DEFAULT_NEXT_ROTATION_FOLDER,
    DEFAULT_PAUSE_IMAGE, DEFAULT_ROTATION_IMAGE,
    DEFAULT_SCENE_PAUSE, DEFAULT_SCENE_STREAM,
    DEFAULT_SCENE_ROTATION_SCREEN, DEFAULT_VLC_SOURCE_NAME,
    DEFAULT_FALLBACK_FAILURE_THRESHOLD, FALLBACK_RETRY_INTERVAL,
    FALLBACK_RETRY_PENDING_ATTEMPTS,
)

# Load environment variables from project root
from config.constants import _PROJECT_ROOT
load_dotenv(os.path.join(_PROJECT_ROOT, '.env'))

logger = logging.getLogger(__name__)

# OBS Configuration
OBS_HOST = os.getenv("OBS_HOST", "127.0.0.1")
OBS_PORT = int(os.getenv("OBS_PORT", 4455))
OBS_PASSWORD = os.getenv("OBS_PASSWORD", "")
OBS_PATH = os.getenv("OBS_PATH", "")  # Path to obs64.exe for freeze recovery
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
            vlc_source_name=VLC_SOURCE_NAME,
        )

        # Services
        self.notification_service = NotificationService(DISCORD_WEBHOOK_URL)
        self.playback_monitor: Optional[PlaybackMonitor] = None

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

        # Prepared rotation manager
        self.prepared_rotation_manager = PreparedRotationManager(
            playlist_manager=self.playlist_manager,
            config_manager=self.config_manager,
            video_registration_queue=self.video_registration_queue,
            shutdown_event=self._shutdown_event,
        )
        self.prepared_rotation_manager.set_download_manager(self.download_manager)

        # Handlers (initialized in _initialize_handlers)
        self.content_switch_handler: Optional[ContentSwitchHandler] = None
        self.temp_playback_handler: Optional[TempPlaybackHandler] = None

        # Expose constants for DashboardHandler (avoids circular imports)
        self._scene_pause = SCENE_PAUSE
        self._scene_stream = SCENE_STREAM
        self._scene_rotation_screen = SCENE_ROTATION_SCREEN
        self._vlc_source_name = VLC_SOURCE_NAME
        self._env_twitch_client_id = TWITCH_CLIENT_ID
        self._env_twitch_client_secret = TWITCH_CLIENT_SECRET
        self._env_kick_client_id = KICK_CLIENT_ID
        self._env_kick_client_secret = KICK_CLIENT_SECRET
        self._env_discord_webhook_url = DISCORD_WEBHOOK_URL

        # State
        self.current_session_id: Optional[int] = None
        self.next_prepared_playlists = None
        self.last_stream_status = None
        self.is_rotating = False
        self._manual_pause = False  # True when paused via dashboard (prevents auto-resume)
        self._rotation_postpone_logged = False
        self._just_resumed_session = False  # Track if we just resumed to skip initial download trigger
        self._shutdown_requested = False
        self._title_refresh_needed = False  # Set by download callback to append preview names to title
        self._start_time = time.time()  # For uptime tracking

        # Prepared rotation overlay state
        self._prepared_rotation_active = False  # True while a prepared rotation is playing
        self._saved_live_video: Optional[str] = None  # Original name of live video that was playing
        self._saved_live_cursor_ms: int = 0  # Cursor position within that video
        self._saved_live_folder: Optional[str] = None  # Path to live folder to restore
        self._restore_cursor_after_prepared = False  # Per-execution flag from dashboard

        # Fallback state — activated when yt-dlp fails repeatedly
        self._fallback_active = False
        self._fallback_needed = False  # Armed by download failures, activated when live empties
        self._fallback_tier: Optional[str] = None  # 'prepared', 'pause'
        self._consecutive_download_failures = 0
        self._fallback_retry_count = 0  # How many retries attempted during current fallback
        self._last_fallback_retry: float = 0.0
        self._fallback_rotation_folders: List[str] = []  # All fallback-marked prepared rotations
        self._fallback_rotation_index: int = 0  # Current position in the cycle
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None  # Set in run()
        
        # Dashboard handler — owns all web-dashboard state / command logic
        self.dashboard_handler = DashboardHandler(self)

        # OBS freeze monitor — detects hung OBS via render frame stall
        self.obs_freeze_monitor = OBSFreezeMonitor(
            obs_exe_path=OBS_PATH or None,
        )

        # Web dashboard client (optional, enabled via env vars)
        self.web_dashboard: Optional[WebDashboardClient] = None
        if WEB_DASHBOARD_URL and WEB_DASHBOARD_API_KEY:
            self.web_dashboard = WebDashboardClient(
                api_key=WEB_DASHBOARD_API_KEY,
                state_provider=self.dashboard_handler.get_dashboard_state,
                command_handler=self.dashboard_handler.handle_command,
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
            on_download_failure=self._on_download_failure,
            on_download_success=self._on_download_success,
        )

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def _set_next_prepared_playlists(self, playlists) -> None:
        """Callback for download manager to set prepared playlists."""
        self.next_prepared_playlists = playlists
        if playlists:
            self._title_refresh_needed = True

    # ── Fallback system ──────────────────────────────────────────

    def _on_download_failure(self) -> None:
        """Called (from download thread) when a background download fails.

        Arms fallback mode after reaching the failure threshold.  Actual
        activation is deferred until live content is exhausted — see
        ``check_for_rotation``.
        """
        self._consecutive_download_failures += 1
        logger.warning(
            f"Download failure #{self._consecutive_download_failures} "
            f"(threshold: {DEFAULT_FALLBACK_FAILURE_THRESHOLD})"
        )
        if (self._consecutive_download_failures >= DEFAULT_FALLBACK_FAILURE_THRESHOLD
                and not self._fallback_active and not self._fallback_needed):
            self._fallback_needed = True
            logger.warning("Fallback armed — will activate when live content is exhausted")

        # Allow the next main-loop iteration to re-trigger a download attempt,
        # but stop once fallback is armed — the fallback retry timer takes over.
        if not self._fallback_needed and not self._fallback_active:
            self.download_manager.downloads_triggered_this_rotation = False

    def _on_download_success(self) -> None:
        """Called (from download thread) when a background download succeeds."""
        if self._consecutive_download_failures > 0:
            logger.info(
                f"Download succeeded — resetting failure counter "
                f"(was {self._consecutive_download_failures})"
            )
        self._consecutive_download_failures = 0
        self._fallback_needed = False
        if self._fallback_active:
            if self._event_loop and self._event_loop.is_running():
                asyncio.run_coroutine_threadsafe(self._deactivate_fallback(), self._event_loop)
            else:
                logger.error("Cannot schedule fallback deactivation — event loop not available")

    async def _activate_fallback(self) -> None:
        """Enter fallback mode — live content is exhausted and downloads are failing.

        Tier 1: Execute a fallback-marked prepared rotation (full title/category).
        Tier 2: Switch to pause scene (nothing to play).

        Called from ``check_for_rotation`` when live empties and
        ``_fallback_needed`` is True, or from the dashboard ``force_fallback``
        command.
        """
        if self._fallback_active:
            return

        self._fallback_needed = False
        logger.warning("===== FALLBACK MODE ACTIVATING =====")

        # Tier 1: Fallback prepared rotations — build cycle list
        fallback_folders = self.prepared_rotation_manager.get_all_fallback_rotations()
        if fallback_folders:
            self._fallback_active = True
            self._fallback_tier = "prepared"
            self._fallback_retry_count = 0
            self._last_fallback_retry = time.time()
            self._fallback_rotation_folders = fallback_folders
            self._fallback_rotation_index = 0
            folder = fallback_folders[0]
            logger.info(
                f"Fallback Tier 1: executing fallback prepared rotation from {folder} "
                f"({len(fallback_folders)} fallback rotation(s) available)"
            )
            self.notification_service.notify_fallback_activated("prepared")
            await self.dashboard_handler.execute_prepared_rotation(folder)
            return

        # Tier 2: Pause screen
        self._fallback_active = True
        self._fallback_tier = "pause"
        self._fallback_retry_count = 0
        self._last_fallback_retry = time.time()
        self._fallback_rotation_folders = []
        self._fallback_rotation_index = 0
        if self.obs_controller:
            self.obs_controller.switch_scene(SCENE_PAUSE)
        logger.warning("Fallback Tier 2: no content available — showing pause screen")
        self.notification_service.notify_fallback_activated("pause")

    async def _deactivate_fallback(self) -> None:
        """Exit fallback mode — downloads have recovered.

        For the "prepared" tier the fallback content may still be playing;
        restore live playback only if new content is actually available.
        For "pause" tier, switch back to the stream scene so the normal
        rotation logic picks up the newly downloaded content.
        """
        if not self._fallback_active:
            return

        previous_tier = self._fallback_tier
        logger.info(f"===== EXITING FALLBACK MODE (was tier: {previous_tier}) =====")

        if previous_tier == "prepared" and self._prepared_rotation_active:
            # Don't call restore_after_prepared_rotation() here — live/ is
            # empty and the downloaded content is in pending/.  Just clear the
            # prepared-rotation flag and let the current fallback content
            # finish naturally.  When all_content_consumed fires, the normal
            # rotation logic will see pending content and rotate into it.
            self._prepared_rotation_active = False
            self.prepared_rotation_manager.complete_execution()

        if previous_tier == "pause" and self.obs_controller:
            self.obs_controller.switch_scene(SCENE_STREAM)

        self._fallback_active = False
        self._fallback_tier = None
        self._fallback_needed = False
        self._consecutive_download_failures = 0
        self._fallback_retry_count = 0
        self._fallback_rotation_folders = []
        self._fallback_rotation_index = 0
        self.notification_service.notify_fallback_deactivated(previous_tier or "unknown")
        logger.info("Fallback mode deactivated — normal operation resumed")

    def _should_retry_fallback_download(self) -> bool:
        """Return True if enough time has elapsed to retry a download in fallback."""
        if not self._fallback_active:
            return False
        return (time.time() - self._last_fallback_retry) >= FALLBACK_RETRY_INTERVAL

    @staticmethod
    def _get_video_files_in(folder: str) -> list:
        """Return video files in a folder (for fallback tier checks)."""
        from config.constants import VIDEO_EXTENSIONS
        if not folder or not os.path.isdir(folder):
            return []
        try:
            return [f for f in os.listdir(folder) if f.lower().endswith(VIDEO_EXTENSIONS)]
        except Exception:
            return []

    def reload_env(self) -> dict:
        """Re-read .env file and update module-level constants + instance attrs.

        Returns a dict summarising what changed so the caller can log it.
        """
        global OBS_HOST, OBS_PORT, OBS_PASSWORD
        global SCENE_PAUSE, SCENE_STREAM, SCENE_ROTATION_SCREEN, VLC_SOURCE_NAME
        global TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET
        global KICK_CLIENT_ID, KICK_CLIENT_SECRET
        global DISCORD_WEBHOOK_URL
        global WEB_DASHBOARD_URL, WEB_DASHBOARD_API_KEY

        # Snapshot old values for diff
        old = {
            "OBS_HOST": OBS_HOST, "OBS_PORT": OBS_PORT, "OBS_PASSWORD": OBS_PASSWORD,
            "SCENE_PAUSE": SCENE_PAUSE, "SCENE_STREAM": SCENE_STREAM,
            "SCENE_ROTATION_SCREEN": SCENE_ROTATION_SCREEN,
            "VLC_SOURCE_NAME": VLC_SOURCE_NAME,
            "TWITCH_CLIENT_ID": TWITCH_CLIENT_ID,
            "TWITCH_CLIENT_SECRET": TWITCH_CLIENT_SECRET,
            "KICK_CLIENT_ID": KICK_CLIENT_ID,
            "KICK_CLIENT_SECRET": KICK_CLIENT_SECRET,
            "DISCORD_WEBHOOK_URL": DISCORD_WEBHOOK_URL,
            "WEB_DASHBOARD_URL": WEB_DASHBOARD_URL,
            "WEB_DASHBOARD_API_KEY": WEB_DASHBOARD_API_KEY,
        }

        # Re-read .env into os.environ (override=True so changed values win)
        load_dotenv(os.path.join(_PROJECT_ROOT, '.env'), override=True)

        # Re-evaluate module-level constants
        OBS_HOST = os.getenv("OBS_HOST", "127.0.0.1")
        OBS_PORT = int(os.getenv("OBS_PORT", 4455))
        OBS_PASSWORD = os.getenv("OBS_PASSWORD", "")
        SCENE_PAUSE = os.getenv("SCENE_PAUSE", os.getenv("SCENE_LIVE", DEFAULT_SCENE_PAUSE))
        SCENE_STREAM = os.getenv("SCENE_STREAM", os.getenv("SCENE_OFFLINE", DEFAULT_SCENE_STREAM))
        SCENE_ROTATION_SCREEN = os.getenv("SCENE_ROTATION_SCREEN", DEFAULT_SCENE_ROTATION_SCREEN)
        VLC_SOURCE_NAME = os.getenv("VLC_SOURCE_NAME", DEFAULT_VLC_SOURCE_NAME)
        TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "")
        TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "")
        KICK_CLIENT_ID = os.getenv("KICK_CLIENT_ID", "")
        KICK_CLIENT_SECRET = os.getenv("KICK_CLIENT_SECRET", "")
        DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
        WEB_DASHBOARD_URL = os.getenv("WEB_DASHBOARD_URL", "")
        WEB_DASHBOARD_API_KEY = os.getenv("WEB_DASHBOARD_API_KEY", "")

        # Build diff
        new = {
            "OBS_HOST": OBS_HOST, "OBS_PORT": OBS_PORT, "OBS_PASSWORD": OBS_PASSWORD,
            "SCENE_PAUSE": SCENE_PAUSE, "SCENE_STREAM": SCENE_STREAM,
            "SCENE_ROTATION_SCREEN": SCENE_ROTATION_SCREEN,
            "VLC_SOURCE_NAME": VLC_SOURCE_NAME,
            "TWITCH_CLIENT_ID": TWITCH_CLIENT_ID,
            "TWITCH_CLIENT_SECRET": TWITCH_CLIENT_SECRET,
            "KICK_CLIENT_ID": KICK_CLIENT_ID,
            "KICK_CLIENT_SECRET": KICK_CLIENT_SECRET,
            "DISCORD_WEBHOOK_URL": DISCORD_WEBHOOK_URL,
            "WEB_DASHBOARD_URL": WEB_DASHBOARD_URL,
            "WEB_DASHBOARD_API_KEY": WEB_DASHBOARD_API_KEY,
        }
        changed = {k: new[k] for k in new if old[k] != new[k]}

        if not changed:
            logger.info("reload_env: .env re-read — no changes detected")
            return changed

        # Mask secret values in logs
        safe = {k: ("****" if "SECRET" in k or "PASSWORD" in k or "API_KEY" in k else v) for k, v in changed.items()}
        logger.info(f"reload_env: changed keys → {safe}")

        # ── Update instance attrs ──
        self._scene_pause = SCENE_PAUSE
        self._scene_stream = SCENE_STREAM
        self._scene_rotation_screen = SCENE_ROTATION_SCREEN
        self._vlc_source_name = VLC_SOURCE_NAME
        self._env_twitch_client_id = TWITCH_CLIENT_ID
        self._env_twitch_client_secret = TWITCH_CLIENT_SECRET
        self._env_kick_client_id = KICK_CLIENT_ID
        self._env_kick_client_secret = KICK_CLIENT_SECRET
        self._env_discord_webhook_url = DISCORD_WEBHOOK_URL

        # ── Update rotation manager scene names ──
        self.rotation_manager._scene_stream = SCENE_STREAM
        self.rotation_manager._scene_pause = SCENE_PAUSE
        self.rotation_manager._scene_rotation = SCENE_ROTATION_SCREEN
        self.rotation_manager._vlc_source = VLC_SOURCE_NAME

        # ── Reconstruct simple services if their config changed ──

        # Discord webhook — just swap the attribute
        if "DISCORD_WEBHOOK_URL" in changed:
            self.notification_service.discord_webhook_url = DISCORD_WEBHOOK_URL
            logger.info("reload_env: updated Discord webhook URL")

        # Twitch live checker
        if "TWITCH_CLIENT_ID" in changed or "TWITCH_CLIENT_SECRET" in changed:
            if TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET:
                self.twitch_live_checker = TwitchLiveChecker(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
                logger.info("reload_env: rebuilt Twitch live checker")
            else:
                self.twitch_live_checker = None
                logger.info("reload_env: Twitch credentials cleared — live checker disabled")

        # Kick live checker
        if "KICK_CLIENT_ID" in changed or "KICK_CLIENT_SECRET" in changed:
            if KICK_CLIENT_ID and KICK_CLIENT_SECRET:
                self.kick_live_checker = KickLiveChecker(KICK_CLIENT_ID, KICK_CLIENT_SECRET)
                logger.info("reload_env: rebuilt Kick live checker")
            else:
                self.kick_live_checker = None
                logger.info("reload_env: Kick credentials cleared — live checker disabled")

        # OBS connection — reconnect if host/port/password changed
        if any(k in changed for k in ("OBS_HOST", "OBS_PORT", "OBS_PASSWORD")):
            logger.info("reload_env: OBS connection config changed — will reconnect on next tick")
            try:
                self.obs_connection.disconnect()
            except Exception:
                pass
            self.obs_connection = OBSConnectionManager(
                host=OBS_HOST, port=OBS_PORT, password=OBS_PASSWORD,
                shutdown_event=self._shutdown_event,
                vlc_source_name=VLC_SOURCE_NAME,
            )
            # obs_controller will be reconstructed on next tick's connect attempt

        return changed

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
            reinitialize_playback_monitor=self._initialize_playback_monitor,
            update_category_after_switch=self._update_category_for_current_video,
            set_pending_seek=self._set_pending_seek,
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

        # Capture playback monitor temp mode
        monitor_was_temp = (
            self.playback_monitor is not None
            and self.playback_monitor._temp_playback_mode
        )

        self._initialize_handlers()

        # OBS restarted — the old stall count and render-frame baseline are
        # stale.  Reset sampling so the freeze monitor starts fresh and
        # doesn't false-positive from pre-restart data.
        self.obs_freeze_monitor._reset_sampling()

        # Restore temp playback state on the new handler instance
        if temp_was_active and self.temp_playback_handler:
            self.temp_playback_handler._active = True
            if temp_session_id is not None:
                self.temp_playback_handler.set_session_id(temp_session_id)
            logger.info("Preserved temp playback active state across OBS reconnect")

        self._initialize_playback_monitor()

        # Restore temp mode on the new playback monitor
        if monitor_was_temp and self.playback_monitor:
            self.playback_monitor.set_temp_playback_mode(True)
            logger.info("Preserved playback monitor temp playback mode across OBS reconnect")

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

    async def _recover_from_obs_freeze(self) -> bool:
        """Full OBS freeze recovery: capture state → kill → relaunch → reconnect → resume.

        Returns:
            True if recovery succeeded, False otherwise.
        """
        monitor = self.obs_freeze_monitor

        # 1. Capture streaming state before we kill OBS
        if self.obs_controller:
            monitor.capture_stream_state(self.obs_controller.obs_client)

        # 2. Save current playback position for seek-after-reconnect
        self._tick_save_playback()

        # 2.5 Suspend playback monitor so events during OBS kill/restart
        # aren't misread as video transitions (which would delete files).
        if self.playback_monitor:
            self.playback_monitor.suspend()

        # 3. Kill + relaunch OBS
        monitor.kill_obs()

        if not monitor.launch_obs(wait_seconds=8.0):
            monitor.mark_recovery_attempted(succeeded=False)
            self.notification_service.notify_automation_error(
                "OBS freeze recovery FAILED — could not relaunch OBS. "
                "Set OBS_PATH in .env if OBS is not in the default location."
            )
            return False

        # 4. Reconnect via WebSocket
        if not self.obs_connection.reconnect(max_retries=5, base_delay=2.0):
            monitor.mark_recovery_attempted(succeeded=False)
            self.notification_service.notify_automation_error(
                "OBS freeze recovery FAILED — OBS relaunched but WebSocket reconnect failed."
            )
            return False

        # 5. Re-initialize handlers (same as normal reconnect)
        self._reinitialize_after_obs_reconnect()
        logger.info("OBS freeze recovery: handlers re-initialized after restart")

        # 6. Ensure all scenes/sources exist and VLC source has its playlist
        if self.obs_controller:
            self.obs_controller.ensure_scenes(
                scene_stream=SCENE_STREAM,
                scene_pause=SCENE_PAUSE,
                scene_rotation=SCENE_ROTATION_SCREEN,
                vlc_source_name=VLC_SOURCE_NAME,
                video_folder=self.config_manager.video_folder,
                pause_image=DEFAULT_PAUSE_IMAGE,
                rotation_image=DEFAULT_ROTATION_IMAGE,
            )
            # Repopulate VLC source playlist from the live folder
            self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, self.config_manager.video_folder)
            logger.info("OBS freeze recovery: scenes and VLC source restored")

        # 7. Switch to the stream scene so OBS shows the right content
        if self.obs_controller:
            try:
                self.obs_controller.switch_scene(SCENE_STREAM)
                logger.info("OBS freeze recovery: switched to stream scene")
            except Exception as e:
                logger.warning(f"OBS freeze recovery: failed to switch scene: {e}")

        # 7. Resume streaming if it was active
        if monitor.was_streaming and self.obs_controller:
            # Give OBS a moment to fully initialize before starting stream
            await asyncio.sleep(3.0)
            if not monitor.resume_streaming(self.obs_controller.obs_client):
                logger.warning("OBS freeze recovery: streaming could not be resumed automatically")
                self.notification_service.notify_automation_error(
                    "OBS restarted successfully but streaming could not be resumed. "
                    "You may need to click Start Streaming manually."
                )

        monitor.mark_recovery_attempted(succeeded=True)
        self.notification_service.notify_automation_info(
            "OBS freeze recovery SUCCEEDED — OBS was restarted and reconnected"
            + (" (streaming resumed)" if monitor.was_streaming else "")
            + ". Automatic recovery remains active for future freezes."
        )
        logger.info("OBS freeze recovery completed successfully")
        return True

    def save_playback_on_exit(self):
        """Save current state when program exits."""
        # Save final playback position for crash recovery
        if self.current_session_id and self.playback_monitor and self.obs_controller:
            try:
                status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
                if status and status.get('media_cursor') is not None:
                    current_video = self.playback_monitor.current_video_original_name
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

    def _initialize_playback_monitor(self, video_folder: Optional[str] = None,
                                     override_current_video: Optional[str] = None):
        """Initialize playback monitor for current rotation.
        
        Args:
            video_folder: Path to the video folder to monitor. If None, uses config default.
            override_current_video: If set, override the monitor's starting video
                instead of defaulting to the first alphabetical file. Used when
                the VLC playlist was reordered for playback resume.
        """
        if not self.obs_controller:
            return
        
        if video_folder is None:
            settings = self.config_manager.get_settings()
            video_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)
        
        if self.playback_monitor is None:
            self.playback_monitor = PlaybackMonitor(
                self.db, self.obs_controller, VLC_SOURCE_NAME,
                event_queue=self.obs_connection.media_event_queue,
                config=self.config_manager, scene_stream=SCENE_STREAM
            )
        else:
            # Update reference after OBS reconnect (new OBSController instance)
            self.playback_monitor.obs_controller = self.obs_controller
        
        self.playback_monitor.initialize(str(video_folder))
        
        # Override the current video pointer if the caller knows which file
        # VLC is actually playing (e.g. reordered playlist for resume).
        if override_current_video and self.playback_monitor:
            self.playback_monitor._current_video = override_current_video
            logger.info(f"Playback monitor current video overridden to: {override_current_video}")

    async def _update_category_for_current_video(self) -> None:
        """Update stream category based on the video currently playing.
        
        Called after playback monitor (re)initialization to ensure the category
        matches the actual video VLC is playing.
        """
        if not self.playback_monitor or not self.stream_manager:
            return
        
        # Ensure any pending video registrations are in DB first
        self.download_manager.process_video_registration_queue()
        
        category = self.playback_monitor.get_category_for_current_video()
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

            new_title = self.playlist_manager.generate_stream_title(
                playlist_names, preview_playlists=self._get_next_rotation_preview_names()
            )
            old_title = session.get('stream_title', '')

            if new_title != old_title:
                await self.stream_manager.update_title(new_title)
                self.db.update_session_stream_title(self.current_session_id, new_title)
                logger.info(f"Stream title updated on config change: '{old_title}' -> '{new_title}'")
            else:
                logger.debug("Config changed but stream title unchanged, skipping title update")
        except Exception as e:
            logger.warning(f"Failed to update stream title on config change: {e}")

    async def _remove_playlist_from_title(self, video_record: dict) -> None:
        """Remove a completed playlist from the stream title.

        Called when the last video of a playlist finishes and playback moves
        to the next playlist.  Updates ``playlists_selected`` in the session
        so the title only shows content that is still playing or upcoming.

        If there is room after removing the completed playlist, names from
        the next prepared rotation are appended as a preview so viewers
        always see what content is coming up.
        """
        try:
            if not self.stream_manager or not self.current_session_id:
                return

            playlist_id = video_record.get('playlist_id')
            if not playlist_id:
                return

            session = self.db.get_current_session()
            if not session:
                return

            playlists_json = session.get('playlists_selected')
            if not playlists_json:
                return

            playlist_ids: list = json.loads(playlists_json)
            if playlist_id not in playlist_ids:
                return

            playlist_ids.remove(playlist_id)
            if not playlist_ids:
                # Don't empty the title entirely — the last playlist will
                # be removed naturally when the next rotation starts.
                return

            # Persist the trimmed list and regenerate the title
            self.db.update_session_playlists_selected(self.current_session_id, playlist_ids)

            playlist_names = []
            for pid in playlist_ids:
                p = self.db.get_playlist(pid)
                if p:
                    playlist_names.append(p['name'])

            if not playlist_names:
                return

            # Gather next-rotation preview names (if downloads are done)
            preview_names = self._get_next_rotation_preview_names()

            new_title = self.playlist_manager.generate_stream_title(
                playlist_names, preview_playlists=preview_names
            )
            old_title = session.get('stream_title', '')

            if new_title != old_title:
                await self.stream_manager.update_title(new_title)
                self.db.update_session_stream_title(self.current_session_id, new_title)
                logger.info(
                    f"Removed completed playlist from title: '{old_title}' -> '{new_title}'"
                )
        except Exception as e:
            logger.warning(f"Failed to remove playlist from title: {e}")

    def _get_next_rotation_preview_names(self) -> list[str]:
        """Return playlist names for the next prepared rotation, if available.

        Used to preview upcoming content in the stream title when there is
        room after the current rotation's playlists.
        """
        if not self.next_prepared_playlists:
            return []
        return [p['name'] for p in self.next_prepared_playlists if p.get('name')]

    async def _refresh_title_with_previews(self) -> None:
        """Regenerate the stream title to include next-rotation preview names.

        Called from the main loop when the background download finishes and
        ``_title_refresh_needed`` is set.  Adds upcoming playlist names to
        the title (space permitting) so viewers see what content is next.
        """
        try:
            if not self.stream_manager or not self.current_session_id:
                return

            session = self.db.get_current_session()
            if not session:
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

            preview_names = self._get_next_rotation_preview_names()
            if not preview_names:
                return  # nothing new to add

            new_title = self.playlist_manager.generate_stream_title(
                playlist_names, preview_playlists=preview_names
            )
            old_title = session.get('stream_title', '')

            if new_title != old_title:
                await self.stream_manager.update_title(new_title)
                self.db.update_session_stream_title(self.current_session_id, new_title)
                logger.info(
                    f"Title updated with next-rotation preview: '{old_title}' -> '{new_title}'"
                )
        except Exception as e:
            logger.warning(f"Failed to refresh title with previews: {e}")

    async def _handle_temp_playback_vlc_refresh(self) -> None:
        """Refresh VLC source at the natural end of a video during temp playback.
        
        Called when the playback monitor detects the last video finished while
        in temp playback mode.  New files may have been downloaded since VLC was
        last loaded — this is the safe moment to reload (video just ended, no
        mid-video disruption for viewers).
        
        If new files exist  → refresh VLC, reinitialize monitor, continue playing.
        If no new files     → delete the finished video, mark consumed, let the
                              normal temp-playback-exit flow handle it.
        """
        if not self.playback_monitor:
            return
        
        settings = self.config_manager.get_settings()
        pending_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
        
        # Stop VLC to release its grip on the file before deleting.
        # A scene switch alone doesn't make VLC release the file handle.
        if self.obs_controller:
            self.obs_controller.switch_scene(SCENE_ROTATION_SCREEN)
            self.obs_controller.stop_vlc_source(VLC_SOURCE_NAME)
            await asyncio.sleep(0.5)
        
        # Delete the finished video now (monitor deferred deletion for us)
        finished_video = self.playback_monitor.current_video
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
            self.playback_monitor.clear_vlc_refresh_flag()
            self._initialize_playback_monitor(pending_folder)
            if self.playback_monitor:
                self.playback_monitor.set_temp_playback_mode(True)
            
            # Update category for the new video
            await self._update_category_for_current_video()
            
            logger.info("Temp playback VLC refreshed — continuing playback")
        else:
            # No new files — mark consumed so normal exit flow kicks in
            logger.info("No new files in pending after last temp video — marking consumed")
            self.playback_monitor.clear_vlc_refresh_flag()
            self.playback_monitor._all_content_consumed = True

    async def _try_recover_session(self) -> None:
        """Attempt to start a fresh session when none is active.

        Called once per tick from ``check_for_rotation()`` when there is no
        current session.  Stays on the pause scene until enough enabled
        playlists are present in playlists.json to meet the configured
        minimum, then kicks off a normal rotation.
        """
        # Ensure we're on the pause scene while waiting
        if self.obs_controller:
            try:
                current_scene = self.obs_controller.get_current_scene()
                if current_scene != self._scene_pause:
                    self.obs_controller.switch_scene(self._scene_pause)
                    logger.info("No active session — switched to pause scene while awaiting playlists")
            except Exception:
                pass

        # Only re-check when config actually changes to avoid per-tick noise
        if not self.config_manager.has_config_changed():
            return

        # Sync any newly-added playlists into the DB
        self.db.sync_playlists_from_config(self.config_manager.get_playlists())

        settings = self.config_manager.get_settings()
        min_playlists = settings.get('min_playlists_per_rotation', 2)

        enabled = [p for p in self.config_manager.get_playlists() if p.get('enabled', True)]
        if len(enabled) < min_playlists:
            logger.debug(
                f"Waiting for playlists: {len(enabled)} enabled, "
                f"need at least {min_playlists}"
            )
            return

        logger.info(
            f"Enough playlists available ({len(enabled)} >= {min_playlists}) "
            f"— starting fresh rotation session"
        )
        if await self.rotation_manager.start_session():
            self.download_manager.downloads_triggered_this_rotation = False
            self.download_manager.background_download_in_progress = False
            await self.rotation_manager.execute_content_switch()

    async def check_for_rotation(self):
        """Check if rotation is needed and handle it."""
        if self.is_rotating:
            return

        session = self.db.get_current_session()
        if not session:
            # No active session — this can happen when playlists were removed
            # mid-rotation causing start_session() to fail, or when the program
            # starts with an empty playlists.json.  Park on the pause scene and
            # periodically try to start a fresh session once enough playlists
            # become available.
            await self._try_recover_session()
            return

        # Check playback monitor for video transitions
        # Skip when on the pause screen — VLC isn't playing so all files appear unlocked
        if self.playback_monitor and self.last_stream_status != "live":
            check_result = self.playback_monitor.check()
            
            if check_result['transition']:
                # Video transition detected - update stream category
                current_video = check_result.get('current_video')
                previous_video = check_result.get('previous_video')
                
                # Log the completed video in playback_log
                if previous_video:
                    self.db.log_playback(previous_video, session.get('id'))

                # Per-playlist last_played: when the playlist changes between
                # consecutive videos the previous playlist's content is done.
                if previous_video and current_video:
                    prev_rec = self.db.get_video_by_filename(previous_video)
                    cur_rec = self.db.get_video_by_filename(current_video)
                    prev_pl = prev_rec.get('playlist_name') if prev_rec else None
                    cur_pl = cur_rec.get('playlist_name') if cur_rec else None
                    if prev_pl and prev_pl != cur_pl:
                        name = self.db.mark_playlist_played_for_video(previous_video)
                        if name:
                            logger.info(f"Marked playlist '{name}' as played (last video transitioned)")
                        # Remove completed playlist from the title — its
                        # content has finished so it should no longer appear.
                        if prev_rec:
                            await self._remove_playlist_from_title(prev_rec)

                # Mark the final playlist as played when all content is consumed.
                # Without this, the last playlist in a rotation is never marked
                # because there's no "next" video to trigger the playlist-change
                # check above.  (execute_content_switch also does this for normal
                # rotations, but temp playback activation bypasses that path.)
                if previous_video and not current_video and check_result.get('all_consumed'):
                    name = self.db.mark_playlist_played_for_video(previous_video)
                    if name:
                        logger.info(f"Marked playlist '{name}' as played (rotation content exhausted)")
                
                if current_video and self.content_switch_handler and self.stream_manager:
                    try:
                        await self.content_switch_handler.update_category_for_video_async(
                            current_video, self.stream_manager
                        )
                        # Optional video transition notification
                        settings = self.config_manager.get_settings()
                        if settings.get('notify_video_transitions', False):
                            cat_dict = self.playback_monitor.get_category_for_current_video() if self.playback_monitor else None
                            cat_label = " / ".join(f"{k}: {v}" for k, v in cat_dict.items()) if cat_dict else None
                            self.notification_service.notify_video_transition(current_video, cat_label)
                    except Exception as e:
                        logger.warning(f"Failed to update category on video transition: {e}")
            
            # Handle VLC refresh during temp playback: last video finished but
            # new files may have been downloaded.  Refresh VLC at this natural
            # transition point (no mid-video disruption) and reinitialize.
            if self.playback_monitor.needs_vlc_refresh:
                await self._handle_temp_playback_vlc_refresh()
                return
        elif self.playback_monitor and self.last_stream_status == "live":
            # Drain stale events every tick while paused so they don't
            # burst-fire false transitions when the streamer goes offline.
            # check() is bypassed entirely during pause, so its internal
            # drain paths never execute.
            self.playback_monitor._drain_queue()

        # Trigger background download only if pending folder is empty and not already triggered
        # Skip on first loop after resume to avoid downloading when resuming into existing rotation
        # Skip entirely during temp playback — pending folder is in use for playback
        # Skip during prepared rotation — live/pending must not be touched
        settings = self.config_manager.get_settings()
        pending_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
        temp_active = self.temp_playback_handler is not None and self.temp_playback_handler.is_active
        if (not temp_active and
            not self._prepared_rotation_active and
            not self.download_manager.downloads_triggered_this_rotation and 
            self.playlist_manager.is_folder_empty(pending_folder) and 
            not self.download_manager.background_download_in_progress and
            not self._just_resumed_session):
            self.download_manager.maybe_start_background_download(self.next_prepared_playlists)
        
        # Clear the resume flag after first loop iteration
        if self._just_resumed_session:
            self._just_resumed_session = False

        # Check if all content is consumed
        all_consumed = self.playback_monitor is not None and self.playback_monitor.all_content_consumed
        has_pending_content = not self.playlist_manager.is_folder_empty(pending_folder)

        # ── Prepared rotation finished → restore live playback ──
        # If we're in fallback mode with a prepared rotation, cycle to the
        # next fallback rotation (or loop back to the first one).
        if all_consumed and self._prepared_rotation_active:
            if self._fallback_active and self._fallback_tier == "prepared":
                # Minimal cleanup — do NOT call restore_after_prepared_rotation()
                # because that tries to point VLC back at the (empty) live folder,
                # causing errors.  We just need to mark the rotation finished so
                # execute_prepared_rotation() can start cleanly.
                self._prepared_rotation_active = False
                self.prepared_rotation_manager.complete_execution()

                # Refresh the list in case rotations were added/removed during fallback
                self._fallback_rotation_folders = self.prepared_rotation_manager.get_all_fallback_rotations()
                if self._fallback_rotation_folders:
                    # Advance to the next fallback rotation (wrap around)
                    self._fallback_rotation_index = (
                        (self._fallback_rotation_index + 1) % len(self._fallback_rotation_folders)
                    )
                    next_folder = self._fallback_rotation_folders[self._fallback_rotation_index]
                    logger.info(
                        f"Fallback cycling to rotation {self._fallback_rotation_index + 1}"
                        f"/{len(self._fallback_rotation_folders)}: {next_folder}"
                    )
                    await self.dashboard_handler.execute_prepared_rotation(next_folder)
                else:
                    # All fallback rotations were removed — fall through to pause
                    logger.warning("No fallback rotations available — switching to pause screen")
                    self._fallback_tier = "pause"
                    if self.obs_controller:
                        self.obs_controller.switch_scene(SCENE_PAUSE)
                    self.notification_service.notify_fallback_activated("pause")
                return
            logger.info("Prepared rotation content finished — restoring live playback")
            await self.dashboard_handler.restore_after_prepared_rotation()
            return
        
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
            
            # Also treat an active background download (DB may not have been
            # updated yet) as "downloads in progress".
            downloads_active = pending_incomplete or self.download_manager.background_download_in_progress
            
            # Activate temp playback if downloads are in progress.
            # activate() handles the case where no files are ready yet by
            # polling until the first file appears (shows rotation screen
            # while waiting so the stream isn't dead).
            if downloads_active and self.temp_playback_handler:
                if pending_has_files:
                    logger.info(f"Long playlist detected downloading ({len(pending_complete_files)} files ready in pending) - activating temp playback")
                else:
                    logger.info("Content exhausted while downloads in progress — activating temp playback (will wait for first file)")
                
                activated = await self.temp_playback_handler.activate(session)
                
                if activated:
                    # Re-initialize playback monitor to watch the pending folder
                    self._initialize_playback_monitor(pending_folder)
                    if self.playback_monitor:
                        self.playback_monitor.set_temp_playback_mode(True)
                    
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

        # ── Fallback activation point ──
        # Live is empty, temp playback can't help, and no rotation content
        # available.  If downloads have been failing, activate fallback now.
        if (all_consumed and not temp_active and not should_rotate
                and self._fallback_needed and not self._fallback_active):
            logger.warning("Live content exhausted and downloads failing — activating fallback")
            await self._activate_fallback()
            return

    async def _check_live_status(self, ignore_streamer: bool) -> None:
        """Check if the streamer is live and toggle pause/stream scenes accordingly.

        Checks both Twitch and Kick if configured. Either platform being live
        triggers a pause. Skipped entirely when neither TARGET_TWITCH_STREAMER
        nor TARGET_KICK_STREAMER is set.

        HTTP calls to Twitch/Kick APIs run in background threads via
        ``asyncio.to_thread`` so they never block the event loop.
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

        # Refresh tokens in background threads (each can block up to 10s)
        if target_twitch and self.twitch_live_checker:
            try:
                await asyncio.to_thread(self.twitch_live_checker.refresh_token_if_needed)
            except Exception as e:
                logger.warning(f"Failed to refresh Twitch app token: {e}")

        if target_kick and self.kick_live_checker:
            try:
                await asyncio.to_thread(self.kick_live_checker.refresh_token_if_needed)
            except Exception as e:
                logger.warning(f"Failed to refresh Kick app token: {e}")

        # Check live status in background threads (each can block up to 10s)
        is_live = False
        if target_twitch and self.twitch_live_checker:
            is_live = await asyncio.to_thread(self.twitch_live_checker.is_stream_live, target_twitch)
        if not is_live and target_kick and self.kick_live_checker:
            is_live = await asyncio.to_thread(self.kick_live_checker.is_stream_live, target_kick)

        if ignore_streamer:
            is_live = False

        if is_live and self.last_stream_status != "live":
            logger.info("Streamer is LIVE — pausing 24/7 stream")
            # Save playback position before pausing so we can resume later
            if self.current_session_id and self.playback_monitor and self.obs_controller:
                try:
                    status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
                    if status and status.get('media_cursor') is not None:
                        current_video = self.playback_monitor.current_video_original_name
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
            self.last_stream_status = "live"
            self.notification_service.notify_streamer_live()
        elif not is_live and self.last_stream_status != "offline":
            if self._manual_pause:
                # Manual pause is active — don't auto-resume when streamer goes offline
                logger.debug("Streamer is OFFLINE but manual pause is active — staying paused")
                return
            logger.info("Streamer is OFFLINE — resuming 24/7 stream")
            # Drain any events that accumulated during the pause and
            # suppress the 'started' event VLC will fire when the scene
            # switch makes VLC visible/active again.
            if self.playback_monitor:
                self.playback_monitor._drain_queue()
                self.playback_monitor._suppress_started += 1
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
        if not (self.current_session_id and self.playback_monitor and self.obs_controller):
            return
        try:
            status = self.obs_controller.get_media_input_status(VLC_SOURCE_NAME)
            if not status or status.get('media_cursor') is None:
                return

            current_video = self.playback_monitor.current_video_original_name
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

    def _set_pending_seek(self, cursor_ms: int, video_name: str) -> None:
        """Schedule a deferred seek — used by temp playback exit to resume position."""
        self._pending_seek_ms = cursor_ms
        self._pending_seek_video = video_name

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
        self._event_loop = asyncio.get_running_loop()

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
        if self.web_dashboard:
            asyncio.create_task(self.web_dashboard.run())
            logger.info("Web dashboard client started")

        # Sync and check for startup conditions
        config_playlists = self.config_manager.get_playlists()
        self.db.sync_playlists_from_config(config_playlists)

        session = self.db.get_current_session()
        settings = self.config_manager.get_settings()
        video_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)

        if not session:
            logger.info("No active session, starting initial rotation")
            if await self.rotation_manager.start_session():
                await self.rotation_manager.execute_content_switch()
        elif not os.path.exists(video_folder) or not os.listdir(video_folder):
            logger.warning(f"Video folder empty/missing: {video_folder}")

            # Temp playback streams from the *pending* folder, so the live
            # folder is expected to be empty.  Route to resume_existing_session
            # which already handles temp playback crash recovery.
            temp_state = self.db.get_temp_playback_state(session['id'])
            if temp_state and temp_state.get('active'):
                logger.info("Temp playback was active — routing to session resume for recovery")
                await self.rotation_manager.resume_existing_session(session, settings)
            else:
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
                                    self._set_next_prepared_playlists(objs)
                                    logger.info(
                                        f"Found completed prepared playlists in "
                                        f"pending, will rotate to: {playlist_list}"
                                    )
                    except Exception as e:
                        logger.warning(f"Failed to check pending playlists: {e}")
                if session.get('id'):
                    self.db.end_session(session['id'])
                if await self.rotation_manager.start_session():
                    await self.rotation_manager.execute_content_switch()
        else:
            await self.rotation_manager.resume_existing_session(session, settings)

        # Main loop
        loop_count = 0
        last_ignore_streamer = False
        while True:
            try:
                settings = self.config_manager.get_settings()
                ignore_streamer = settings.get('ignore_streamer', False)

                ignore_streamer_changed = (ignore_streamer != last_ignore_streamer)
                if ignore_streamer_changed:
                    logger.info(f"ignore_streamer changed to {ignore_streamer}, forcing live status recheck")
                last_ignore_streamer = ignore_streamer

                if loop_count % max(int(settings.get('live_check_interval_seconds', 30)), 5) == 0 or ignore_streamer_changed:
                    await self._check_live_status(ignore_streamer)

                self.download_manager.process_video_registration_queue()
                self.download_manager.process_pending_database_operations()
                self._tick_save_playback()

                # OBS freeze detection — check render frame progression.
                # Runs even when disconnected: WebSocket timeouts count as
                # stalls so a truly frozen OBS will accumulate enough to
                # trigger kill-and-relaunch recovery.
                if self.obs_controller:
                    obs_client = self.obs_controller.obs_client if self.obs_controller.is_connected else None
                    freeze_status = self.obs_freeze_monitor.check(obs_client)
                    if freeze_status == "frozen":
                        logger.error("OBS FREEZE DETECTED — initiating recovery")
                        self.notification_service.notify_automation_error(
                            "OBS appears frozen (no new render frames for ~60s). Attempting automatic recovery..."
                        )
                        if await self._recover_from_obs_freeze():
                            continue
                        else:
                            logger.error("OBS freeze recovery failed")
                    elif freeze_status == "frozen_final":
                        self.notification_service.notify_automation_error(
                            "OBS froze AGAIN after prior recovery. Automatic restart disabled — manual intervention required."
                        )

                # Proactive OBS health check — detect disconnect even though
                # OBSController swallows exceptions internally
                if self.obs_controller and not self.obs_controller.is_connected:
                    logger.warning("OBS connection lost (detected via health check)")
                    self.notification_service.notify_automation_error("OBS disconnected, attempting reconnect...")
                    if self.obs_connection.reconnect(max_retries=3):
                        self._reinitialize_after_obs_reconnect()
                        logger.info("OBS reconnected, handlers re-initialized")
                        continue
                    else:
                        # Reconnect exhausted retries — attempt freeze recovery
                        # (kill + relaunch) as a last resort before giving up.
                        logger.warning("OBS reconnect failed — attempting freeze recovery (kill + relaunch)")
                        if await self._recover_from_obs_freeze():
                            continue
                        logger.error("Failed to reconnect to OBS, shutting down")
                        break

                # Monitor temp playback for download completion
                if self.temp_playback_handler and self.temp_playback_handler.is_active:
                    current_time = time.time()
                    if current_time - self.temp_playback_handler._last_folder_check >= 2.0:
                        await self.temp_playback_handler.monitor()
                        self.temp_playback_handler._last_folder_check = current_time

                await self.check_for_rotation()

                # Check for scheduled prepared rotations (every 30 seconds)
                if loop_count % 30 == 0:
                    scheduled_folder = self.prepared_rotation_manager.check_scheduled()
                    if scheduled_folder and not self.prepared_rotation_manager.is_executing:
                        logger.info(f"Scheduled prepared rotation ready — executing: {scheduled_folder}")
                        await self.dashboard_handler.execute_prepared_rotation(scheduled_folder)

                # Fallback retry: escalating strategy
                #   1) First N retries: re-attempt the same download (yt-dlp
                #      resumes partials via --continue).
                #   2) After N retries: wipe pending folder and try a
                #      completely fresh rotation with different playlists.
                if self._should_retry_fallback_download():
                    self._last_fallback_retry = time.time()
                    self._fallback_retry_count += 1

                    fb_settings = self.config_manager.get_settings()
                    fb_pending = fb_settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)

                    if self._fallback_retry_count <= FALLBACK_RETRY_PENDING_ATTEMPTS:
                        logger.info(
                            f"Fallback retry {self._fallback_retry_count}/{FALLBACK_RETRY_PENDING_ATTEMPTS} "
                            f"— retrying pending download"
                        )
                    else:
                        logger.info(
                            f"Fallback retry {self._fallback_retry_count} "
                            f"(past pending threshold) — trying fresh rotation"
                        )
                        # Wipe pending so we don't re-download the same bad playlists
                        self.playlist_manager.cleanup_temp_downloads(fb_pending)
                        for fn in os.listdir(fb_pending) if os.path.isdir(fb_pending) else []:
                            fp = os.path.join(fb_pending, fn)
                            try:
                                if os.path.isfile(fp):
                                    os.unlink(fp)
                            except Exception:
                                pass
                        # Reset so new playlists are selected
                        self.next_prepared_playlists = None

                    # Allow the download to actually trigger
                    self.download_manager.downloads_triggered_this_rotation = False
                    self.download_manager.maybe_start_background_download(
                        self.next_prepared_playlists
                    )

                if self.config_manager.has_config_changed():
                    logger.info("Config changed, syncing...")
                    self.db.sync_playlists_from_config(self.config_manager.get_playlists())
                    await self._apply_config_changes_to_stream()

                if self._title_refresh_needed:
                    # Don't push preview titles while fallback content is
                    # still playing — the downloaded playlists won't start
                    # until the current fallback rotation finishes naturally.
                    if not self._fallback_active:
                        self._title_refresh_needed = False
                        await self._refresh_title_with_previews()

                if self._shutdown_requested:
                    await self._shutdown_cleanup()
                    break

            except Exception as e:
                error_msg = str(e)
                # Detect OBS disconnection and auto-reconnect
                if any(hint in error_msg.lower() for hint in ('websocket', 'connection', 'socket', 'connect')):
                    logger.warning(f"OBS connection lost: {e}")
                    self.notification_service.notify_automation_error(f"OBS disconnected: {error_msg}")
                    if self.obs_connection.reconnect(max_retries=3):
                        self._reinitialize_after_obs_reconnect()
                        logger.info("OBS reconnected, handlers re-initialized")
                        continue
                    else:
                        # Reconnect failed — try freeze recovery as last resort
                        logger.warning("OBS reconnect failed — attempting freeze recovery (kill + relaunch)")
                        if await self._recover_from_obs_freeze():
                            continue
                        logger.error("Failed to reconnect to OBS, shutting down")
                        break
                logger.error(f"Error in main loop: {e}", exc_info=True)
                self.notification_service.notify_automation_error(error_msg)

            loop_count += 1
            await asyncio.sleep(1)
