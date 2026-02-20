"""OBS Freeze Monitor — detects a hung OBS process via render frame stall.

Polls OBS WebSocket GetStats to track `renderTotalFrames`.  If the counter
fails to advance across several consecutive checks (~60 s), OBS is
considered frozen and recovery is triggered: kill the process, relaunch it,
reconnect via WebSocket, and optionally resume streaming.

Recovery is retried on future freezes if a prior recovery succeeded.
If recovery fails, further automatic restarts are blocked to avoid loops.
"""

import logging
import os
import subprocess
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Default OBS install locations (Windows)
_DEFAULT_OBS_PATHS = [
    os.path.expandvars(r"%ProgramFiles%\obs-studio\bin\64bit\obs64.exe"),
    os.path.expandvars(r"%ProgramFiles(x86)%\obs-studio\bin\64bit\obs64.exe"),
]

# How often we sample render frames (seconds)
_POLL_INTERVAL = 20

# How many consecutive stalled polls before declaring frozen
_STALL_THRESHOLD = 3  # 3 × 20 s = 60 s of no new frames


class OBSFreezeMonitor:
    """Monitors OBS render output and recovers from process freezes."""

    def __init__(self, obs_exe_path: Optional[str] = None) -> None:
        """
        Args:
            obs_exe_path: Explicit path to obs64.exe.  If *None*, the monitor
                          tries common install locations automatically.
        """
        self._obs_exe = obs_exe_path or self._find_obs_executable()

        # Sampling state
        self._last_render_frames: Optional[int] = None
        self._stall_count: int = 0
        self._last_check_time: float = 0.0

        # Recovery state
        self._recovery_attempted: bool = False
        self._recovery_succeeded: bool = False

        # Streaming state captured before kill
        self._was_streaming: bool = False

    # ── Public API ────────────────────────────────────────────────

    def check(self, obs_client) -> Optional[str]:
        """Sample render frames and return a recovery action if needed.

        Call this every tick (~1 s) from the main loop.  Internally it only
        polls OBS every ``_POLL_INTERVAL`` seconds.

        Args:
            obs_client: An ``obsws_python.ReqClient`` instance.

        Returns:
            ``None``      — everything normal, or not time to check yet.
            ``"frozen"``  — OBS is frozen and recovery should be attempted.
            ``"frozen_final"`` — OBS froze again after a prior recovery;
                                 no further automatic recovery will happen.
        """
        now = time.monotonic()
        if now - self._last_check_time < _POLL_INTERVAL:
            return None
        self._last_check_time = now

        try:
            stats = obs_client.get_stats()
            render_total = stats.render_total_frames  # type: ignore[attr-defined]
        except Exception as e:
            # If we can't even reach OBS, the existing disconnect detector
            # in automation_controller will handle it — not our job here.
            logger.debug(f"OBS freeze monitor: GetStats failed ({e}), skipping check")
            self._reset_sampling()
            return None

        if self._last_render_frames is None:
            # First sample — just record baseline
            self._last_render_frames = render_total
            return None

        if render_total > self._last_render_frames:
            # Frames advanced — all good
            self._last_render_frames = render_total
            self._stall_count = 0
            return None

        # Stalled
        self._stall_count += 1
        logger.warning(
            f"OBS freeze monitor: render frames stalled "
            f"({self._stall_count}/{_STALL_THRESHOLD}) — "
            f"renderTotalFrames={render_total}"
        )

        if self._stall_count >= _STALL_THRESHOLD:
            self._stall_count = 0
            self._last_render_frames = None

            if self._recovery_attempted:
                # Already tried once — don't loop
                logger.error(
                    "OBS freeze detected AGAIN after prior recovery — "
                    "not restarting. Manual intervention required."
                )
                return "frozen_final"

            return "frozen"

        return None

    def capture_stream_state(self, obs_client) -> bool:
        """Capture whether OBS is currently streaming (call before kill).

        Returns:
            True if OBS was actively streaming.
        """
        try:
            status = obs_client.get_stream_status()
            self._was_streaming = status.output_active  # type: ignore[attr-defined]
            logger.info(f"OBS freeze recovery: stream was {'active' if self._was_streaming else 'inactive'}")
            return self._was_streaming
        except Exception as e:
            logger.warning(f"Failed to capture OBS stream status before kill: {e}")
            self._was_streaming = False
            return False

    @property
    def was_streaming(self) -> bool:
        """Whether OBS was streaming when we last captured state."""
        return self._was_streaming

    def kill_obs(self) -> bool:
        """Kill the OBS process.

        Returns:
            True if a process was found and killed.
        """
        logger.warning("OBS freeze recovery: killing OBS process...")
        try:
            if os.name == "nt":
                # Windows: taskkill /F /IM obs64.exe
                result = subprocess.run(
                    ["taskkill", "/F", "/IM", "obs64.exe"],
                    capture_output=True, text=True, timeout=10,
                )
                killed = result.returncode == 0
            else:
                # Linux/macOS: pkill -9 obs
                result = subprocess.run(
                    ["pkill", "-9", "obs"],
                    capture_output=True, text=True, timeout=10,
                )
                killed = result.returncode == 0

            if killed:
                logger.info("OBS freeze recovery: OBS process killed")
                # Reset sampling baseline — renderTotalFrames will restart from 0
                self._reset_sampling()
            else:
                logger.warning(f"OBS freeze recovery: taskkill returned non-zero: {result.stderr.strip()}")
            return killed
        except Exception as e:
            logger.error(f"OBS freeze recovery: failed to kill OBS: {e}")
            return False

    def launch_obs(self, wait_seconds: float = 8.0) -> bool:
        """Launch the OBS executable and wait for it to initialize.

        Args:
            wait_seconds: How long to wait after launching before returning.

        Returns:
            True if launch was initiated (does not guarantee OBS is ready).
        """
        if not self._obs_exe:
            logger.error(
                "OBS freeze recovery: cannot launch OBS — executable path unknown. "
                "Set OBS_PATH in your .env file."
            )
            return False

        if not os.path.isfile(self._obs_exe):
            logger.error(f"OBS freeze recovery: OBS executable not found at {self._obs_exe}")
            return False

        logger.info(f"OBS freeze recovery: launching OBS from {self._obs_exe}...")
        try:
            self._clear_crash_sentinel()
            subprocess.Popen(
                [self._obs_exe, "--minimize-to-tray", "--disable-missing-files-check"],
                cwd=os.path.dirname(self._obs_exe),
                start_new_session=True,
            )
            logger.info(f"OBS freeze recovery: waiting {wait_seconds}s for OBS to initialize...")
            time.sleep(wait_seconds)
            return True
        except Exception as e:
            logger.error(f"OBS freeze recovery: failed to launch OBS: {e}")
            return False

    def resume_streaming(self, obs_client) -> bool:
        """Start streaming in OBS if it was active before the kill.

        Args:
            obs_client: The reconnected ``obsws_python.ReqClient``.

        Returns:
            True if streaming was resumed successfully.
        """
        if not self._was_streaming:
            logger.info("OBS freeze recovery: OBS was not streaming before freeze — skipping StartStream")
            return True  # Nothing to do

        logger.info("OBS freeze recovery: resuming streaming (StartStream)...")
        try:
            obs_client.start_stream()
            logger.info("OBS freeze recovery: StartStream sent successfully")
            return True
        except Exception as e:
            logger.error(f"OBS freeze recovery: failed to start stream: {e}")
            return False

    def mark_recovery_attempted(self, succeeded: bool = True) -> None:
        """Record that a recovery cycle was attempted and reset sampling baseline.
        
        After OBS restarts, renderTotalFrames resets to near-zero.
        We must clear the old baseline so the monitor doesn't compare
        against the pre-restart frame count and false-positive.

        If recovery succeeded, the monitor remains armed for future freezes.
        If it failed, further automatic recovery is blocked to avoid loops.
        """
        self._recovery_attempted = not succeeded  # Only block future recovery on failure
        self._recovery_succeeded = succeeded
        self._reset_sampling()

    def reset(self) -> None:
        """Reset all state (for testing or manual override)."""
        self._last_render_frames = None
        self._stall_count = 0
        self._last_check_time = 0.0
        self._recovery_attempted = False
        self._recovery_succeeded = False
        self._was_streaming = False

    # ── Internal helpers ──────────────────────────────────────────

    def _reset_sampling(self) -> None:
        """Reset sample state without touching recovery flags."""
        self._last_render_frames = None
        self._stall_count = 0

    @staticmethod
    def _clear_crash_sentinel() -> None:
        """Delete OBS's sentinel files so it doesn't show the safe-mode dialog.

        OBS creates a ``run_<uuid>`` file in ``.sentinel/`` on startup
        and removes it on clean exit.  Stale sentinels from a forced kill
        trigger the crash recovery prompt.  We wipe the directory before
        relaunching.
        """
        sentinel_dir = os.path.join(
            os.environ.get("APPDATA", ""), "obs-studio", ".sentinel"
        )
        if not os.path.isdir(sentinel_dir):
            return
        try:
            count = 0
            for entry in os.listdir(sentinel_dir):
                filepath = os.path.join(sentinel_dir, entry)
                if os.path.isfile(filepath):
                    os.remove(filepath)
                    count += 1
            if count:
                logger.info(f"OBS freeze recovery: cleared {count} stale sentinel file(s)")
        except Exception as e:
            logger.warning(f"OBS freeze recovery: failed to clear sentinel files: {e}")

    @staticmethod
    def _find_obs_executable() -> Optional[str]:
        """Try to locate obs64.exe on the system."""
        for path in _DEFAULT_OBS_PATHS:
            if os.path.isfile(path):
                logger.info(f"OBS freeze monitor: found OBS at {path}")
                return path
        logger.warning(
            "OBS freeze monitor: could not auto-detect OBS executable. "
            "Set OBS_PATH in .env for freeze recovery to work."
        )
        return None
