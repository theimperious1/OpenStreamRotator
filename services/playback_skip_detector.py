import time
import logging
from datetime import datetime, timedelta
from typing import Optional
from core.database import DatabaseManager
from controllers.obs_controller import OBSController

logger = logging.getLogger(__name__)


class PlaybackSkipDetector:
    """Detects video playback skips and recalculates rotation timing."""

    SKIP_MARGIN_MS = 5000  # 5 second margin for VLC reporting variations

    def __init__(self, db: DatabaseManager, obs_controller: OBSController, vlc_source_name: str):
        """
        Initialize skip detector.
        
        Args:
            db: DatabaseManager for session updates
            obs_controller: OBSController for media status queries
            vlc_source_name: Name of VLC source in OBS
        """
        self.db = db
        self.obs_controller = obs_controller
        self.vlc_source_name = vlc_source_name
        
        self.last_known_playback_position_ms = 0
        self.last_playback_check_time: Optional[float] = None

    def initialize(self):
        """Initialize detector with current VLC position."""
        media_status = self.obs_controller.get_media_input_status(self.vlc_source_name)
        if media_status:
            self.last_known_playback_position_ms = media_status.get('media_cursor', 0) or 0
        self.last_playback_check_time = time.time()
        logger.debug("Playback skip detector initialized")

    def reset(self):
        """Reset detector for new rotation."""
        self.last_known_playback_position_ms = 0
        self.last_playback_check_time = None
        logger.debug("Playback skip detector reset")

    def check_for_skip(self, session_id: Optional[int] = None) -> tuple[bool, Optional[dict]]:
        """
        Check if playback has skipped ahead and recalculate timing.
        
        Returns:
            Tuple of (skip_detected, skip_info)
            skip_info contains: time_skipped_seconds, new_finish_time
        """
        if not self.obs_controller:
            return False, None
        
        media_status = self.obs_controller.get_media_input_status(self.vlc_source_name)
        if not media_status:
            return False, None
        
        current_position_ms = media_status.get('media_cursor')
        total_duration_ms = media_status.get('media_duration')
        
        if current_position_ms is None or total_duration_ms is None:
            return False, None
        
        # First check initialization
        if self.last_playback_check_time is None:
            self.last_playback_check_time = time.time()
            self.last_known_playback_position_ms = current_position_ms
            return False, None
        
        # Calculate expected vs actual position advance
        time_elapsed_seconds = time.time() - self.last_playback_check_time
        expected_position_delta_ms = time_elapsed_seconds * 1000
        position_delta_ms = current_position_ms - self.last_known_playback_position_ms
        
        # Calculate excess advance
        excess_advance_ms = position_delta_ms - expected_position_delta_ms
        
        # Check if skip detected
        if excess_advance_ms > self.SKIP_MARGIN_MS:
            time_skipped_seconds = excess_advance_ms / 1000
            
            logger.info(
                f"Playback skip detected: jumped {excess_advance_ms}ms more than expected "
                f"(expected {expected_position_delta_ms:.0f}ms, got {position_delta_ms}ms advance). "
                f"Position: {self.last_known_playback_position_ms}ms -> {current_position_ms}ms. "
                f"Excess skipped: {time_skipped_seconds:.1f}s"
            )
            
            # Calculate new finish time
            remaining_ms = total_duration_ms - current_position_ms
            remaining_seconds = remaining_ms / 1000
            new_finish_time = datetime.now() + timedelta(seconds=remaining_seconds)
            
            # Update session if provided
            if session_id:
                self.db.update_session_times(
                    session_id,
                    new_finish_time.isoformat(),
                    (new_finish_time - timedelta(minutes=30)).isoformat()
                )
                logger.info(f"Updated session {session_id} with new finish time: {new_finish_time.isoformat()}")
            
            # Update tracking
            self.last_known_playback_position_ms = current_position_ms
            self.last_playback_check_time = time.time()
            
            skip_info = {
                "time_skipped_seconds": time_skipped_seconds,
                "new_finish_time": new_finish_time,
                "new_finish_time_str": new_finish_time.strftime('%H:%M:%S')
            }
            
            return True, skip_info
        
        # No skip, update tracking
        self.last_known_playback_position_ms = current_position_ms
        self.last_playback_check_time = time.time()
        return False, None
