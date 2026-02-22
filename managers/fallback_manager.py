"""Fallback content manager.

Provides three-tier emergency content when yt-dlp downloads fail
repeatedly:

  Tier 1 — **Fallback folder**:  Pre-loaded videos in ``content/fallback/``
           are played in a no-delete loop until downloads recover.
  Tier 2 — **Loop remaining**:   If no fallback videos exist but the live
           folder still has content, stop deleting finished videos and
           loop them instead.
  Tier 3 — **Pause screen**:    Nothing to play at all — switch to the
           pause scene so the stream isn't a black screen.

An OBS text overlay (``OSR Alert``) is shown on the stream scene while
any fallback tier is active, informing the operator what's happening.

The manager re-checks downloads on a configurable interval and
automatically exits fallback mode once a download succeeds.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from enum import Enum, auto
from typing import Optional, Callable, TYPE_CHECKING

from config.constants import (
    DEFAULT_FALLBACK_FOLDER,
    DEFAULT_VIDEO_FOLDER,
    DEFAULT_FALLBACK_FAILURE_THRESHOLD,
    VIDEO_EXTENSIONS,
)

if TYPE_CHECKING:
    from controllers.obs_controller import OBSController
    from services.notification_service import NotificationService

logger = logging.getLogger(__name__)


class FallbackTier(Enum):
    NONE = auto()
    FALLBACK_FOLDER = auto()   # Tier 1
    LOOP_REMAINING = auto()    # Tier 2
    PAUSE_SCREEN = auto()      # Tier 3


# Retry interval while in fallback (seconds) — how often we attempt a fresh
# download to see if yt-dlp is back.
_RETRY_INTERVAL = 300  # 5 minutes


class FallbackManager:
    """Manages emergency fallback content when downloads fail."""

    def __init__(
        self,
        obs_controller: Optional[OBSController],
        notification_service: NotificationService,
        *,
        fallback_folder: str = DEFAULT_FALLBACK_FOLDER,
        scene_stream: str = "OSR Stream",
        scene_pause: str = "OSR Pause screen",
        scene_rotation: str = "OSR Rotation screen",
        vlc_source_name: str = "OSR Playlist",
        alert_source_name: str = "OSR Alert",
        failure_threshold: int = DEFAULT_FALLBACK_FAILURE_THRESHOLD,
    ):
        self.obs_controller = obs_controller
        self.notification_service = notification_service
        self.fallback_folder = fallback_folder
        self._scene_stream = scene_stream
        self._scene_pause = scene_pause
        self._scene_rotation = scene_rotation
        self._vlc_source = vlc_source_name
        self._alert_source = alert_source_name
        self._failure_threshold = failure_threshold

        # State
        self._tier: FallbackTier = FallbackTier.NONE
        self._consecutive_failures: int = 0
        self._last_retry_time: float = 0.0
        self._active: bool = False

        # Callbacks (set by AutomationController)
        self._set_no_delete_mode: Optional[Callable[[bool], None]] = None
        self._reinit_file_lock_monitor: Optional[Callable[[str], None]] = None

    # ── Public properties ────────────────────────────────────────────

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def tier(self) -> FallbackTier:
        return self._tier

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    # ── Callbacks ────────────────────────────────────────────────────

    def set_callbacks(
        self,
        set_no_delete_mode: Callable[[bool], None],
        reinit_file_lock_monitor: Callable[[str], None],
    ) -> None:
        self._set_no_delete_mode = set_no_delete_mode
        self._reinit_file_lock_monitor = reinit_file_lock_monitor

    # ── Failure tracking ─────────────────────────────────────────────

    def record_download_failure(self) -> bool:
        """Record a download failure. Returns True if the threshold is now
        reached and fallback should be activated."""
        self._consecutive_failures += 1
        logger.warning(
            f"Download failure #{self._consecutive_failures} "
            f"(threshold: {self._failure_threshold})"
        )
        return self._consecutive_failures >= self._failure_threshold

    def record_download_success(self) -> None:
        """Reset the failure counter after a successful download."""
        if self._consecutive_failures > 0:
            logger.info(
                f"Download succeeded — resetting failure counter "
                f"(was {self._consecutive_failures})"
            )
        self._consecutive_failures = 0

    # ── Activation ───────────────────────────────────────────────────

    async def activate(self, live_folder: str = DEFAULT_VIDEO_FOLDER) -> FallbackTier:
        """Enter fallback mode.  Determines the best tier and switches
        OBS content accordingly.  Returns the tier that was activated."""

        if self._active:
            logger.debug("Fallback already active, skipping re-activation")
            return self._tier

        fallback_files = self._get_video_files(self.fallback_folder)
        live_files = self._get_video_files(live_folder)

        if fallback_files:
            tier = FallbackTier.FALLBACK_FOLDER
        elif live_files:
            tier = FallbackTier.LOOP_REMAINING
        else:
            tier = FallbackTier.PAUSE_SCREEN

        self._tier = tier
        self._active = True
        self._last_retry_time = time.time()

        if tier == FallbackTier.FALLBACK_FOLDER:
            await self._activate_fallback_folder()
        elif tier == FallbackTier.LOOP_REMAINING:
            await self._activate_loop_remaining()
        else:
            await self._activate_pause_screen()

        logger.info(f"Fallback activated — tier: {tier.name}")
        return tier

    async def _activate_fallback_folder(self) -> None:
        """Tier 1: Point VLC at the fallback folder (no-delete loop)."""
        if not self.obs_controller:
            return

        # Switch to rotation screen while we reload VLC
        self.obs_controller.switch_scene(self._scene_rotation)
        await asyncio.sleep(1.0)

        self.obs_controller.stop_vlc_source(self._vlc_source)
        await asyncio.sleep(0.5)

        success, _ = self.obs_controller.update_vlc_source(
            self._vlc_source, self.fallback_folder
        )
        if not success:
            logger.error("Failed to load fallback folder into VLC")
            await self._activate_pause_screen()
            return

        await asyncio.sleep(0.3)
        self.obs_controller.switch_scene(self._scene_stream)

        # Re-init file lock monitor on fallback folder, no-delete mode
        if self._reinit_file_lock_monitor:
            self._reinit_file_lock_monitor(self.fallback_folder)
        if self._set_no_delete_mode:
            self._set_no_delete_mode(True)

        # Show overlay
        alert_msg = "⚠ FALLBACK MODE — Downloads failing, playing backup content"
        self.obs_controller.show_alert_text(
            self._scene_stream, self._alert_source, alert_msg
        )

        self.notification_service.notify_fallback_activated("fallback_folder")

    async def _activate_loop_remaining(self) -> None:
        """Tier 2: Keep playing whatever is in the live folder but stop
        deleting finished videos so they loop."""
        if self._set_no_delete_mode:
            self._set_no_delete_mode(True)

        if self.obs_controller:
            alert_msg = "⚠ FALLBACK MODE — Downloads failing, looping remaining content"
            self.obs_controller.show_alert_text(
                self._scene_stream, self._alert_source, alert_msg
            )

        self.notification_service.notify_fallback_activated("loop_remaining")

    async def _activate_pause_screen(self) -> None:
        """Tier 3: Nothing to play — switch to the pause scene."""
        self._tier = FallbackTier.PAUSE_SCREEN

        if self.obs_controller:
            self.obs_controller.switch_scene(self._scene_pause)
            alert_msg = "⚠ FALLBACK MODE — No content available, stream paused"
            self.obs_controller.show_alert_text(
                self._scene_stream, self._alert_source, alert_msg
            )

        self.notification_service.notify_fallback_activated("pause_screen")

    # ── Deactivation ─────────────────────────────────────────────────

    async def deactivate(self, live_folder: str = DEFAULT_VIDEO_FOLDER) -> None:
        """Exit fallback mode.  Restores normal playback from the live folder."""
        if not self._active:
            return

        previous_tier = self._tier
        logger.info(f"Exiting fallback mode (was tier: {previous_tier.name})")

        # Hide OBS alert overlay
        if self.obs_controller:
            self.obs_controller.hide_alert_text(self._scene_stream, self._alert_source)

        # If we were on the fallback folder, switch VLC back to live
        if previous_tier == FallbackTier.FALLBACK_FOLDER:
            if self.obs_controller:
                self.obs_controller.switch_scene(self._scene_rotation)
                await asyncio.sleep(1.0)
                self.obs_controller.stop_vlc_source(self._vlc_source)
                await asyncio.sleep(0.5)
                self.obs_controller.update_vlc_source(self._vlc_source, live_folder)
                await asyncio.sleep(0.3)
                self.obs_controller.switch_scene(self._scene_stream)

            if self._reinit_file_lock_monitor:
                self._reinit_file_lock_monitor(live_folder)

        # If we were on pause screen, switch back to stream
        if previous_tier == FallbackTier.PAUSE_SCREEN:
            if self.obs_controller:
                self.obs_controller.switch_scene(self._scene_stream)

        # Restore normal delete behaviour
        if self._set_no_delete_mode:
            self._set_no_delete_mode(False)

        self._active = False
        self._tier = FallbackTier.NONE
        self._consecutive_failures = 0

        self.notification_service.notify_fallback_deactivated()
        logger.info("Fallback mode deactivated — normal operation resumed")

    # ── Periodic retry check ─────────────────────────────────────────

    def should_retry_download(self) -> bool:
        """Returns True if enough time has passed to retry a download."""
        if not self._active:
            return False
        return (time.time() - self._last_retry_time) >= _RETRY_INTERVAL

    def mark_retry_attempted(self) -> None:
        """Update the last-retry timestamp so we wait before trying again."""
        self._last_retry_time = time.time()

    # ── Startup check ────────────────────────────────────────────────

    @staticmethod
    def check_fallback_folder(fallback_folder: str = DEFAULT_FALLBACK_FOLDER) -> bool:
        """Return True if the fallback folder has at least one video.
        Used at startup to warn the operator."""
        if not os.path.isdir(fallback_folder):
            return False
        for f in os.listdir(fallback_folder):
            if f.lower().endswith(VIDEO_EXTENSIONS):
                return True
        return False

    @staticmethod
    def startup_warning(fallback_folder: str = DEFAULT_FALLBACK_FOLDER) -> None:
        """Log a warning if the fallback folder is empty at startup."""
        if not FallbackManager.check_fallback_folder(fallback_folder):
            logger.warning(
                f"Fallback folder is empty ({fallback_folder}). "
                f"If yt-dlp breaks, there will be no backup content to play. "
                f"Consider adding some videos to this folder."
            )

    # ── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _get_video_files(folder: str) -> list[str]:
        if not folder or not os.path.isdir(folder):
            return []
        try:
            return [
                f for f in sorted(os.listdir(folder))
                if f.lower().endswith(VIDEO_EXTENSIONS)
            ]
        except Exception:
            return []
