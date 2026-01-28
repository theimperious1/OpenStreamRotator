import time
import logging
from typing import Optional
from core.database import DatabaseManager

logger = logging.getLogger(__name__)


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
