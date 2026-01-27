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
        self.total_rotation_duration_ms = 0  # Total duration of current rotation session
        self.original_finish_time: Optional[datetime] = None  # Original finish time - don't extend past this
        self.cumulative_playback_ms = 0  # Cumulative playback across all videos in playlist (accounts for video transitions)

    def initialize(self, total_duration_seconds: int = 0, original_finish_time: Optional[datetime] = None):
        """Initialize detector with current VLC position and rotation duration.
        
        Args:
            total_duration_seconds: Total duration of the rotation session in seconds.
                                   If 0, will try to use current video duration.
            original_finish_time: Original estimated finish time for this session.
                                 Used as ceiling to prevent extending rotation indefinitely.
        """
        media_status = self.obs_controller.get_media_input_status(self.vlc_source_name)
        if media_status:
            self.last_known_playback_position_ms = media_status.get('media_cursor', 0) or 0
        self.last_playback_check_time = time.time()
        self.total_rotation_duration_ms = total_duration_seconds * 1000
        self.original_finish_time = original_finish_time
        self.cumulative_playback_ms = 0
        logger.info(f"Playback skip detector initialized (total rotation: {total_duration_seconds}s, original finish: {original_finish_time})")

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
        
        # Detect video transition (position reset when VLC moves to next video in playlist)
        # When this happens, add the previous video duration to cumulative
        if position_delta_ms < -1000:  # Large negative jump = video transition
            logger.info(f"Video transition detected: position went from {self.last_known_playback_position_ms}ms to {current_position_ms}ms")
            # Previous video ended, so add its duration to cumulative
            self.cumulative_playback_ms += self.last_known_playback_position_ms
            logger.info(f"Cumulative playback now: {self.cumulative_playback_ms}ms ({self.cumulative_playback_ms/1000:.1f}s)")
            position_delta_ms = current_position_ms  # Current position in new video
        
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
            
            # Calculate remaining using CUMULATIVE playback position across all videos
            # cumulative_playback_ms = all previous videos' durations
            # + current_position_ms = position within current video
            total_consumed_ms = self.cumulative_playback_ms + current_position_ms
            remaining_ms = self.total_rotation_duration_ms - total_consumed_ms
            
            logger.info(
                f"Cumulative consumed: {total_consumed_ms/1000:.1f}s "
                f"(previous videos: {self.cumulative_playback_ms/1000:.1f}s + current video: {current_position_ms/1000:.1f}s). "
                f"Total duration: {self.total_rotation_duration_ms/1000:.1f}s. "
                f"Remaining: {remaining_ms/1000:.1f}s"
            )
            
            remaining_seconds = max(0, remaining_ms / 1000)  # Don't go negative
            new_finish_time = datetime.now() + timedelta(seconds=remaining_seconds)
            
            logger.info(f"Skip recalculation - new finish would be: {new_finish_time}, original: {self.original_finish_time}")
            
            # Cap finish time: never extend past original session finish time
            if self.original_finish_time and new_finish_time > self.original_finish_time:
                logger.warning(f"Skip would extend finish time to {new_finish_time}, capping at original: {self.original_finish_time}")
                new_finish_time = self.original_finish_time
            elif not self.original_finish_time:
                logger.warning("No original finish time set - capping disabled!")
            
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
