import time
import logging
import os
from typing import Optional, TYPE_CHECKING

from core.database import DatabaseManager

if TYPE_CHECKING:
    from controllers.obs_controller import OBSController

logger = logging.getLogger(__name__)

# OBS VLC source name
VLC_SOURCE_NAME = os.getenv("VLC_SOURCE_NAME", "Playlist")
SCENE_LIVE = os.getenv("SCENE_LIVE", "Pause screen")


class PlaybackTracker:
    """Tracks video playback time for rotation sessions."""

    def __init__(self, db: DatabaseManager):
        """
        Initialize playback tracker.
        
        Args:
            db: DatabaseManager instance for session updates
        """
        self.db = db
        self.playback_start_time: Optional[float] = None
        self.total_playback_seconds: int = 0
        self._last_save_time: float = 0

    def start_tracking(self):
        """Start playback time tracking."""
        self.playback_start_time = time.time()
        self.total_playback_seconds = 0
        logger.debug("Playback tracking started")

    def pause_tracking(self):
        """Pause tracking and accumulate time."""
        if self.playback_start_time is None:
            return
        
        elapsed = time.time() - self.playback_start_time
        self.total_playback_seconds += int(elapsed)
        self.playback_start_time = time.time()

    def resume_tracking(self):
        """Resume playback tracking."""
        self.playback_start_time = time.time()

    def update_session(self, session_id: int):
        """Update session in database with current playback time."""
        if session_id:
            self.db.update_session_playback(session_id, self.total_playback_seconds)

    def reset(self):
        """Reset tracking for new session."""
        self.playback_start_time = None
        self.total_playback_seconds = 0
        logger.debug("Playback tracker reset")

    def get_elapsed_seconds(self) -> int:
        """Get elapsed seconds since start of current tracking period."""
        if self.playback_start_time is None:
            return 0
        return int(time.time() - self.playback_start_time)

    def get_total_seconds(self) -> int:
        """Get total accumulated playback seconds."""
        return self.total_playback_seconds + self.get_elapsed_seconds()

    def is_tracking(self) -> bool:
        """Check if currently tracking playback."""
        return self.playback_start_time is not None

    def save_on_exit(self, session_id: Optional[int], obs_controller: Optional['OBSController']) -> None:
        """Save current playback position when program exits.
        
        Args:
            session_id: Current session ID to save position for
            obs_controller: OBS controller to get VLC position from
        """
        if not session_id:
            logger.debug("No active session, skipping playback save")
            return
        
        try:
            if not obs_controller:
                logger.warning("No OBS controller available, skipping playback save")
                return
            
            current_position_ms = obs_controller.get_playback_position_ms(VLC_SOURCE_NAME)
            if current_position_ms is None:
                logger.warning("VLC position is None, skipping save")
                return
            
            playback_seconds = current_position_ms / 1000
            self.db.update_session_playback(session_id, int(playback_seconds))
            logger.info(f"Saved playback position: {playback_seconds:.1f}s")
            
            if obs_controller.switch_scene(SCENE_LIVE):
                logger.info("Switched to pause scene on exit")
            
        except Exception as e:
            logger.error(f"Failed to save playback on exit: {e}")

    def auto_save_position(self, session_id: Optional[int], obs_controller: Optional['OBSController']) -> None:
        """Auto-save playback position for power loss resilience.
        
        Args:
            session_id: Current session ID to save position for
            obs_controller: OBS controller to get VLC position from
        """
        if not session_id or not obs_controller:
            return
        
        try:
            current_position_ms = obs_controller.get_playback_position_ms(VLC_SOURCE_NAME)
            if current_position_ms is not None:
                playback_seconds = current_position_ms / 1000
                self.db.update_session_playback(session_id, int(playback_seconds))
                self._last_save_time = time.time()
        except Exception as e:
            logger.debug(f"Auto-save playback failed (non-critical): {e}")
