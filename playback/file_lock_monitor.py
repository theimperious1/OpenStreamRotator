"""
File Lock Monitor - Detects video transitions by monitoring file lock state.

Instead of position-based skip detection, this monitors which video file VLC
currently has locked. When a file's lock is released, it means VLC has moved
to the next video. The freed file is deleted and the next video is identified.

For the last video in rotation, VLC cursor/duration polling is used since
VLC loops the last video (never releasing the lock).
"""

import logging
import os
import time
from typing import Optional, TYPE_CHECKING

from config.constants import VIDEO_EXTENSIONS
from utils.video_utils import strip_ordering_prefix, resolve_category_for_video

if TYPE_CHECKING:
    from controllers.obs_controller import OBSController
    from core.database import DatabaseManager
    from config.config_manager import ConfigManager

logger = logging.getLogger(__name__)


def is_file_locked(filepath: str) -> bool:
    """Check if a file is locked by another process (e.g., VLC).
    
    On Windows, attempts to rename the file to itself. This fails if another
    process has the file open with a lock.
    
    Args:
        filepath: Full path to the file to check
    
    Returns:
        True if the file is locked, False if it's free
    """
    try:
        os.rename(filepath, filepath)
        return False
    except OSError:
        return True


class FileLockMonitor:
    """Monitors video file locks to detect playback transitions.
    
    Approach:
    - Track which file VLC currently has locked (= currently playing)
    - When that file's lock frees → VLC moved to next video → delete the freed file
    - For the last video: poll VLC cursor vs duration to detect completion
    - Videos are sorted alphabetically; prefixed with 'XX_' for playlist ordering
    """

    def __init__(self, db: 'DatabaseManager', obs_controller: 'OBSController', vlc_source_name: str,
                 config: Optional['ConfigManager'] = None):
        self.db = db
        self.obs_controller = obs_controller
        self.vlc_source_name = vlc_source_name
        self.config = config
        
        self.video_folder: str = ""
        self._current_video: Optional[str] = None  # Filename of currently playing video
        self._all_content_consumed: bool = False
        self._temp_playback_mode: bool = False
        self._last_video_duration_seconds: int = 0  # Cached duration for last-video polling
        
        # Debounce: when a file is freed, wait one extra check cycle before deleting.
        # This avoids false positives from VLC briefly releasing a file between reads.
        self._pending_transition_file: Optional[str] = None

    def initialize(self, video_folder: str) -> None:
        """Initialize the monitor for a new rotation.
        
        Scans the video folder and waits for VLC to lock a file before returning.
        This prevents the race condition where the monitor starts checking before
        VLC has grabbed its first file, which would cause a premature deletion.
        
        Falls back to assuming the first file after a 10-second timeout.
        
        Args:
            video_folder: Path to the folder containing videos
        """
        self.video_folder = video_folder
        self._current_video = None
        self._all_content_consumed = False
        self._pending_transition_file = None
        self._last_video_duration_seconds = 0
        
        files = self._get_video_files()
        if not files:
            logger.warning("File lock monitor initialized with empty folder")
            logger.info(f"File lock monitor tracking 0 videos in {video_folder}")
            return
        
        # Poll until VLC locks a file (up to 10 seconds)
        timeout = 10.0
        poll_interval = 0.5
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            for f in files:
                filepath = os.path.join(self.video_folder, f)
                if is_file_locked(filepath):
                    self._current_video = f
                    logger.info(f"File lock monitor initialized: current video = {f} (locked after {time.time() - start_time:.1f}s)")
                    logger.info(f"File lock monitor tracking {len(files)} videos in {video_folder}")
                    return
            time.sleep(poll_interval)
        
        # Timeout: fall back to first file
        self._current_video = files[0]
        logger.warning(f"File lock monitor: no file locked after {timeout}s, assuming first: {files[0]}")
        logger.info(f"File lock monitor tracking {len(files)} videos in {video_folder}")

    def reset(self) -> None:
        """Reset the monitor state."""
        self._current_video = None
        self._all_content_consumed = False
        self._pending_transition_file = None
        self._last_video_duration_seconds = 0
        logger.debug("File lock monitor reset")

    def set_temp_playback_mode(self, enabled: bool) -> None:
        """Enable or disable temp playback mode.
        
        Args:
            enabled: Whether temp playback is active
        """
        self._temp_playback_mode = enabled
        logger.info(f"File lock monitor temp playback mode: {'enabled' if enabled else 'disabled'}")

    @property
    def all_content_consumed(self) -> bool:
        """Whether all videos have been played and the rotation is complete."""
        return self._all_content_consumed

    @property
    def current_video(self) -> Optional[str]:
        """Filename of the currently playing video (with prefix if present)."""
        return self._current_video

    @property
    def current_video_original_name(self) -> Optional[str]:
        """Original filename (prefix stripped) of the currently playing video.
        
        Use this for database lookups.
        """
        if self._current_video:
            return strip_ordering_prefix(self._current_video)
        return None

    def check(self) -> dict:
        """Check for video transitions by monitoring file locks.
        
        Should be called every ~1 second from the main loop.
        
        Returns:
            dict with:
            - transition: bool - whether a video transition occurred
            - previous_video: str or None - original filename of video that just finished
            - current_video: str or None - original filename of video now playing
            - all_consumed: bool - whether all content has been played
        """
        result = {
            'transition': False,
            'previous_video': None,
            'current_video': strip_ordering_prefix(self._current_video) if self._current_video else None,
            'all_consumed': False,
        }
        
        if not self.video_folder or self._all_content_consumed:
            result['all_consumed'] = self._all_content_consumed
            return result
        
        if not self._current_video:
            # No current video tracked - try to find one
            files = self._get_video_files()
            if not files:
                self._all_content_consumed = True
                result['all_consumed'] = True
                return result
            self._current_video = files[0]
            result['current_video'] = strip_ordering_prefix(self._current_video)
            return result
        
        filepath = os.path.join(self.video_folder, self._current_video)
        
        # Handle missing file (externally deleted)
        if not os.path.exists(filepath):
            logger.warning(f"Current video file missing: {self._current_video}")
            files = self._get_video_files()
            if files:
                self._current_video = files[0]
                result['current_video'] = strip_ordering_prefix(self._current_video)
            else:
                self._all_content_consumed = True
                result['all_consumed'] = True
                self._current_video = None
            return result
        
        files = self._get_video_files()
        is_last_video = len(files) <= 1
        
        # --- Last video handling: poll VLC cursor vs duration ---
        if is_last_video:
            return self._check_last_video(result)
        
        # --- Normal handling: check if current file's lock has been freed ---
        if is_file_locked(filepath):
            # Still playing, no transition
            self._pending_transition_file = None
            return result
        
        # File is free - VLC has moved on
        # Use debounce: require two consecutive "free" checks to confirm
        if self._pending_transition_file != self._current_video:
            self._pending_transition_file = self._current_video
            return result  # Wait for confirmation on next check
        
        # Confirmed: file has been free for 2 consecutive checks
        self._pending_transition_file = None
        previous_video = self._current_video
        previous_original = strip_ordering_prefix(previous_video)
        
        # Delete the finished video
        self._delete_video(filepath)
        
        # Update VLC source to remove the deleted file
        self._update_vlc_source()
        
        # Get next video
        files = self._get_video_files()
        if files:
            self._current_video = files[0]
            result['transition'] = True
            result['previous_video'] = previous_original
            result['current_video'] = strip_ordering_prefix(self._current_video)
            logger.info(f"Video transition: {previous_original} -> {result['current_video']}")
        else:
            # No more files - all consumed
            self._current_video = None
            self._all_content_consumed = True
            result['transition'] = True
            result['previous_video'] = previous_original
            result['current_video'] = None
            result['all_consumed'] = True
            logger.info(f"Final video finished: {previous_original} - all content consumed")
        
        return result

    def _check_last_video(self, result: dict) -> dict:
        """Handle the last video in rotation using VLC cursor polling.
        
        VLC loops the last video so it never releases the lock. Instead,
        we monitor the VLC cursor position and compare to the video duration.
        When cursor reaches the end, mark as consumed.
        
        Args:
            result: The result dict to populate
        
        Returns:
            Updated result dict
        """
        if not self.obs_controller:
            return result
        
        media_status = self.obs_controller.get_media_input_status(self.vlc_source_name)
        if not media_status:
            return result
        
        cursor_ms = media_status.get('media_cursor')
        duration_ms = media_status.get('media_duration')
        
        if cursor_ms is None or duration_ms is None:
            return result
        
        # Cache duration for logging
        self._last_video_duration_seconds = (duration_ms or 0) // 1000
        
        # Detect when VLC wraps around (cursor resets to near 0 from near the end)
        # This means the video completed one full play and is looping
        # We consider it "done" when cursor wraps around (goes from high to low)
        # Use a threshold: if cursor is in the last 3 seconds, video is about to end
        if duration_ms > 0 and cursor_ms is not None:
            remaining_ms = duration_ms - cursor_ms
            
            if remaining_ms <= 1500:
                # Very close to the end - mark as consumed on next wrap
                # Actually, just mark as consumed now. The video is essentially done.
                previous_original = strip_ordering_prefix(self._current_video) if self._current_video else None
                
                logger.info(
                    f"Last video nearly complete: {previous_original} "
                    f"(cursor={cursor_ms}ms, duration={duration_ms}ms, remaining={remaining_ms}ms)"
                )
                
                # Delete the last video
                if self._current_video:
                    filepath = os.path.join(self.video_folder, self._current_video)
                    # Try to delete - may fail if VLC still has lock
                    if not is_file_locked(filepath):
                        self._delete_video(filepath)
                
                self._all_content_consumed = True
                self._current_video = None
                result['transition'] = True
                result['previous_video'] = previous_original
                result['current_video'] = None
                result['all_consumed'] = True
        
        return result

    def _get_video_files(self) -> list[str]:
        """Get video files in folder sorted alphabetically.
        
        With prefix ordering (01_, 02_), this ensures playlist grouping.
        """
        if not self.video_folder or not os.path.exists(self.video_folder):
            return []
        
        try:
            files = []
            for filename in sorted(os.listdir(self.video_folder)):
                if filename.lower().endswith(VIDEO_EXTENSIONS):
                    files.append(filename)
            return files
        except Exception as e:
            logger.warning(f"Failed to list video files in {self.video_folder}: {e}")
            return []

    def _delete_video(self, filepath: str) -> bool:
        """Delete a completed video file.
        
        Args:
            filepath: Full path to the video file
        
        Returns:
            True if deleted successfully
        """
        try:
            filename = os.path.basename(filepath)
            os.remove(filepath)
            logger.info(f"Deleted completed video: {filename}")
            return True
        except PermissionError:
            filename = os.path.basename(filepath)
            logger.warning(f"Cannot delete {filename} - file still locked, will retry next cycle")
            return False
        except Exception as e:
            filename = os.path.basename(filepath)
            logger.error(f"Failed to delete video {filename}: {e}")
            return False

    def _update_vlc_source(self) -> None:
        """Update VLC source to reflect current folder contents after deletion."""
        if not self.obs_controller:
            return
        
        try:
            success, _ = self.obs_controller.update_vlc_source(
                self.vlc_source_name, self.video_folder
            )
            if success:
                files = self._get_video_files()
                logger.debug(f"Updated VLC source: {len(files)} videos remaining")
        except Exception as e:
            logger.error(f"Failed to update VLC source after deletion: {e}")

    def get_category_for_current_video(self) -> Optional[str]:
        """Get the stream category for the currently playing video.
        
        Returns:
            Category name, or None if unable to determine
        """
        if not self._current_video or not self.config:
            return None
        return resolve_category_for_video(self._current_video, self.db, self.config)
