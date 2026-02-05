import time
import logging
import os
from datetime import datetime, timedelta
from typing import Optional
from core.database import DatabaseManager
from controllers.obs_controller import OBSController

logger = logging.getLogger(__name__)


class PlaybackSkipDetector:
    """Detects video playback skips and recalculates rotation timing."""

    SKIP_MARGIN_MS = 5000  # 5 second margin for VLC reporting variations

    def __init__(self, db: DatabaseManager, obs_controller: OBSController, vlc_source_name: str, video_folder: str = "", content_switch_handler=None, stream_manager=None):
        """
        Initialize skip detector.
        
        Args:
            db: DatabaseManager for session updates
            obs_controller: OBSController for media status queries
            vlc_source_name: Name of VLC source in OBS
            video_folder: Path to the folder containing videos for deletion on transition
            content_switch_handler: Optional ContentSwitchHandler for category updates on video transition
            stream_manager: Optional StreamManager for category updates on video transition
        """
        self.db = db
        self.obs_controller = obs_controller
        self.vlc_source_name = vlc_source_name
        self.video_folder = video_folder
        self.content_switch_handler = content_switch_handler
        self.stream_manager = stream_manager
        
        self.last_known_playback_position_ms = 0
        self.last_playback_check_time: Optional[float] = None
        self.total_rotation_duration_ms = 0  # Total duration of current rotation session
        self.original_finish_time: Optional[datetime] = None  # Original finish time - don't extend past this
        self.cumulative_playback_ms = 0  # Cumulative playback across all videos in playlist (accounts for video transitions)
        self._all_content_consumed = False  # Flag set when final video transitions
        self._resume_seek_pending = False  # Flag to skip cumulative increment on first position check after resume
        self._grace_period_checks = 0  # Grace period to allow VLC to stabilize after initialization
        
        # Playlist tracking for reliable video deletion
        # These track the actual playlist sent to VLC, not folder contents
        self._vlc_playlist: list[str] = []  # List of video filenames in the order VLC received them
        self._playlist_position: int = 0  # Current position in the playlist (0-indexed)
        self._temp_playback_mode: bool = False  # Whether we're in temp playback mode (enables VLC refresh)
        self._vlc_refresh_callback = None  # Callback to refresh VLC when playlist exhausted during temp playback
        self._vlc_refresh_needed = False  # Flag set when VLC refresh is needed (checked by automation controller)
        self._position_change_callback = None  # Callback to notify when playlist position changes (for persistence)

    def initialize(self, total_duration_seconds: int = 0, original_finish_time: Optional[datetime] = None, resume_position_ms: int = 0):
        """Initialize detector with current VLC position and rotation duration.
        
        Args:
            total_duration_seconds: Total duration of the rotation session in seconds.
                                   If 0, will try to use current video duration.
            original_finish_time: Original estimated finish time for this session.
                                 Used as ceiling to prevent extending rotation indefinitely.
            resume_position_ms: If resuming from a paused position, the position to initialize from (in ms).
                               Used to properly calculate remaining time for PC restart scenarios.
        """
        media_status = self.obs_controller.get_media_input_status(self.vlc_source_name)
        if media_status:
            self.last_known_playback_position_ms = media_status.get('media_cursor', 0) or 0
        self.last_playback_check_time = time.time()
        self.total_rotation_duration_ms = total_duration_seconds * 1000
        self.original_finish_time = original_finish_time
        self.cumulative_playback_ms = 0
        self._grace_period_checks = 0  # Reset grace period on initialization
        
        # If resuming from a paused position, set cumulative to indicate we've already consumed that much
        # This way remaining time calculation will be correct when skip detection triggers
        if resume_position_ms > 0:
            self.cumulative_playback_ms = resume_position_ms
            self._resume_seek_pending = True  # Flag the next position check to handle seek
            # Set grace period to prevent premature _all_content_consumed flag on first transition check
            # This gives VLC time to stabilize after loading/seeking
            self._grace_period_checks = 2  # Allow 2 checks before setting flag
            logger.info(f"Initialized skip detector with resume position: {resume_position_ms}ms ({resume_position_ms/1000:.1f}s), grace period enabled")
        
        logger.info(f"Playback skip detector initialized (total rotation: {total_duration_seconds}s, original finish: {original_finish_time})")

    def reset(self):
        """Reset detector for new rotation."""
        self.last_known_playback_position_ms = 0
        self.last_playback_check_time = None
        self.cumulative_playback_ms = 0
        self._all_content_consumed = False
        self._resume_seek_pending = False
        self._grace_period_checks = 0
        # Reset playlist tracking
        self._vlc_playlist = []
        self._playlist_position = 0
        self._temp_playback_mode = False
        self._position_change_callback = None
        logger.debug("Playback skip detector reset")

    def set_handlers(self, content_switch_handler, stream_manager):
        """Set handlers for category updates on video transitions.
        
        Args:
            content_switch_handler: ContentSwitchHandler for updating stream category
            stream_manager: StreamManager for category updates
        """
        self.content_switch_handler = content_switch_handler
        self.stream_manager = stream_manager

    def set_vlc_playlist(self, playlist: list[str], reset_position: bool = True):
        """Set the VLC playlist for tracking which video is playing.
        
        This should be called whenever update_vlc_source is called, with the same
        list of filenames (not full paths, just filenames) in the same order.
        
        Args:
            playlist: List of video filenames in the order they were sent to VLC
            reset_position: Whether to reset position to 0 (True for new playlist, False for resume)
        """
        self._vlc_playlist = playlist.copy()
        if reset_position:
            self._playlist_position = 0
        logger.info(f"Skip detector playlist set: {len(playlist)} videos, position={self._playlist_position}")

    def set_temp_playback_mode(self, enabled: bool, refresh_callback=None):
        """Enable or disable temp playback mode.
        
        In temp playback mode, when the playlist is exhausted and new files are
        available in the folder, the refresh_callback will be called to update VLC.
        
        Args:
            enabled: Whether temp playback mode is active
            refresh_callback: Async callback to refresh VLC source when playlist exhausted
        """
        self._temp_playback_mode = enabled
        self._vlc_refresh_callback = refresh_callback
        logger.info(f"Temp playback mode: {'enabled' if enabled else 'disabled'}")

    def set_position_change_callback(self, callback):
        """Set callback to be called when playlist position changes.
        
        Used for persisting temp playback position to database for crash recovery.
        
        Args:
            callback: Function that takes new_position (int) as argument
        """
        self._position_change_callback = callback

    def get_current_video_from_playlist(self) -> Optional[str]:
        """Get the filename of the current video based on playlist tracking.
        
        Returns:
            Filename of current video, or None if playlist not set or position invalid
        """
        if not self._vlc_playlist or self._playlist_position >= len(self._vlc_playlist):
            return None
        return self._vlc_playlist[self._playlist_position]

    def is_on_last_known_video(self) -> bool:
        """Check if we're on the last video in the tracked VLC playlist."""
        if not self._vlc_playlist:
            return False
        return self._playlist_position >= len(self._vlc_playlist) - 1

    def _update_category_for_current_video(self):
        """Update stream category based on currently playing video."""
        if not self.content_switch_handler or not self.stream_manager:
            return
        
        try:
            current_video = self.get_current_video_filename()
            if current_video:
                self.content_switch_handler.update_category_by_video(current_video, self.stream_manager)
        except Exception as e:
            logger.debug(f"Could not update category on video transition: {e}")

    def get_current_video_filename(self) -> Optional[str]:
        """Get the filename of the currently playing video.
        
        Uses playlist tracking if available (more reliable), otherwise falls back
        to folder scanning (less reliable during active downloads).
        
        Returns:
            Filename of current video, or None if unable to determine
        """
        # Prefer playlist tracking if available
        if self._vlc_playlist:
            return self.get_current_video_from_playlist()
        
        # Fallback to folder scanning (legacy behavior)
        video_files = self._get_video_files_in_order()
        if not video_files:
            return None
        
        # The first file is the current one being played
        return video_files[0]

    def _get_video_files_in_order(self) -> list[str]:
        """Get list of video files in folder, sorted alphabetically."""
        if not self.video_folder or not os.path.exists(self.video_folder):
            return []
        
        video_extensions = ('.mp4', '.mkv', '.avi', '.webm', '.flv', '.mov', '.webm')
        video_files = []
        
        try:
            for filename in sorted(os.listdir(self.video_folder)):
                if filename.lower().endswith(video_extensions):
                    video_files.append(filename)
        except Exception as e:
            logger.warning(f"Failed to list video files: {e}")
        
        return video_files

    def _delete_completed_video(self, video_filename: str, update_vlc: bool = False) -> bool:
        """Delete a completed video file from the video folder.
        
        Args:
            video_filename: Name of the video file to delete
            update_vlc: Whether to update OBS VLC source after deletion (usually False
                       since we track playlist position instead of rescanning folder)
        
        Returns:
            True if deleted successfully, False otherwise
        """
        if not self.video_folder:
            return False
        
        try:
            video_path = os.path.join(self.video_folder, video_filename)
            if os.path.exists(video_path):
                os.remove(video_path)
                logger.info(f"Deleted completed video: {video_filename}")
                
                # Only update OBS VLC source if explicitly requested
                # Normally we don't do this because it restarts the playlist
                if update_vlc and self.obs_controller:
                    success, _ = self.obs_controller.update_vlc_source(self.vlc_source_name, self.video_folder)
                    if success:
                        logger.info(f"Updated OBS VLC source to remove {video_filename}")
                
                return True
            else:
                logger.warning(f"Video file not found for deletion: {video_filename}")
                return False
        except Exception as e:
            logger.error(f"Failed to delete video {video_filename}: {e}")
            return False

    def check_for_skip(self, session_id: Optional[int] = None) -> tuple[bool, Optional[dict]]:
        """
        Check if playback has skipped ahead and recalculate timing.
        
        Returns:
            Tuple of (skip_detected, skip_info)
            skip_info contains: time_skipped_seconds, new_finish_time
        """
        if not self.obs_controller:
            logger.warning("No OBS controller available for skip detection")
            return False, None
        
        media_status = self.obs_controller.get_media_input_status(self.vlc_source_name)
        if not media_status:
            logger.warning(f"Could not get media status for '{self.vlc_source_name}' - VLC may not be reporting playback data")
            return False, None
        
        current_position_ms = media_status.get('media_cursor')
        total_duration_ms = media_status.get('media_duration')
        
        if current_position_ms is None or total_duration_ms is None:
            logger.warning(f"Incomplete media status - cursor={current_position_ms}ms, duration={total_duration_ms}ms")
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
        
        # Handle resume/seek pending: skip the seek operation that positions us at the resume point
        if self._resume_seek_pending and position_delta_ms > 0:
            # This is the seek to resume position - don't treat it as content consumption
            logger.info(f"Resume seek detected: VLC seeked to {current_position_ms}ms (resume point)")
            logger.info(f"Skipping cumulative increment to prevent double-counting - already initialized with resume position")
            self.last_known_playback_position_ms = current_position_ms
            self.last_playback_check_time = time.time()
            self._resume_seek_pending = False  # Clear the flag, next check will be normal playback tracking
            return False, None
        
        # Detect backwards skip (user rewinding) - reset tracking to new position
        if position_delta_ms < -1000:  # Large negative jump
            # Check if this is a video transition or user rewind
            # Video transition: position goes to near 0 (start of next video)
            # User rewind: position goes to some arbitrary earlier point
            if current_position_ms > 1000:  # Not at start of video = user rewind
                logger.info(f"Backwards skip detected: position went from {self.last_known_playback_position_ms}ms to {current_position_ms}ms (user rewound)")
                logger.info(f"Resetting skip detection to {current_position_ms}ms to prevent double-counting rewatched content")
                # Reset: treat current position as new baseline
                self.last_known_playback_position_ms = current_position_ms
                self.last_playback_check_time = time.time()
                return False, None
            else:
                # Position near 0 = video transition
                logger.info(f"Video transition detected: position went from {self.last_known_playback_position_ms}ms to {current_position_ms}ms")
                
                # Determine which video just finished using playlist tracking
                video_to_delete = None
                remaining_in_playlist = 0
                
                if self._vlc_playlist:
                    # Use playlist tracking (reliable)
                    if self._playlist_position < len(self._vlc_playlist):
                        video_to_delete = self._vlc_playlist[self._playlist_position]
                        remaining_in_playlist = len(self._vlc_playlist) - self._playlist_position - 1
                        logger.info(f"Playlist tracking: position {self._playlist_position}, video: {video_to_delete}, remaining: {remaining_in_playlist}")
                else:
                    # Fallback to folder scanning (legacy, less reliable)
                    video_files = self._get_video_files_in_order()
                    if video_files:
                        video_to_delete = video_files[0]
                        remaining_in_playlist = len(video_files) - 1
                
                if video_to_delete and remaining_in_playlist > 0:
                    # Not the last video - delete and advance position
                    self.cumulative_playback_ms += self.last_known_playback_position_ms
                    logger.info(f"Cumulative playback now: {self.cumulative_playback_ms}ms ({self.cumulative_playback_ms/1000:.1f}s)")
                    
                    # Delete the video that just finished and update VLC source
                    self._delete_completed_video(video_to_delete, update_vlc=False)
                    
                    # Advance playlist position BEFORE updating VLC
                    if self._vlc_playlist:
                        self._playlist_position += 1
                        logger.info(f"Advanced playlist position to {self._playlist_position}")
                        
                        # Notify callback for persistence (crash recovery)
                        if self._position_change_callback:
                            try:
                                self._position_change_callback(self._playlist_position)
                            except Exception as e:
                                logger.error(f"Error in position change callback: {e}")
                    
                    # Update VLC with current tracked playlist (excluding deleted video)
                    # In temp playback, always use tracked playlist to avoid adding new files mid-playback
                    if self._temp_playback_mode and self.obs_controller and self._vlc_playlist:
                        remaining_playlist = self._vlc_playlist[self._playlist_position:]
                        success, _ = self.obs_controller.update_vlc_source(self.vlc_source_name, self.video_folder, playlist=remaining_playlist)
                        if success:
                            logger.info(f"Updated VLC with tracked playlist ({len(remaining_playlist)} videos remaining)")
                    elif self.obs_controller:
                        # Normal playback: scan folder
                        success, _ = self.obs_controller.update_vlc_source(self.vlc_source_name, self.video_folder)
                        if success:
                            logger.info(f"Updated OBS VLC source to remove {video_to_delete}")
                    
                    # Update category for newly playing video
                    self._update_category_for_current_video()
                    
                elif video_to_delete:
                    # This is the last video in the tracked playlist
                    logger.info(f"On last video in playlist: {video_to_delete}")
                    
                    # Check if we're in temp playback mode and there are new files available
                    if self._temp_playback_mode and self._vlc_refresh_callback:
                        # Check if there are new files in the folder that aren't in our playlist
                        current_folder_files = self._get_video_files_in_order()
                        new_files_available = len(current_folder_files) > 1  # More than just the current video
                        
                        if new_files_available:
                            logger.info(f"New files available in folder ({len(current_folder_files)} files), triggering VLC refresh")
                            # Delete the current video first
                            self.cumulative_playback_ms += self.last_known_playback_position_ms
                            self._delete_completed_video(video_to_delete, update_vlc=False)
                            # Set flag for automation controller to handle the async refresh callback
                            # This allows the callback to execute in the proper async context
                            self._vlc_refresh_needed = True
                        else:
                            # No new files, set all content consumed flag
                            logger.info(f"Not deleting {video_to_delete} - it's the last video and no new files available")
                            if self._grace_period_checks <= 0:
                                self._all_content_consumed = True
                            else:
                                self._grace_period_checks -= 1
                                logger.info(f"Grace period active: {self._grace_period_checks} checks remaining")
                    else:
                        # Not in temp playback mode, normal behavior
                        logger.info(f"Not deleting {video_to_delete} - it's the last video in playlist")
                        if self._grace_period_checks <= 0:
                            self._all_content_consumed = True
                        else:
                            self._grace_period_checks -= 1
                            logger.info(f"Grace period active: {self._grace_period_checks} checks remaining before allowing rotation trigger")
                
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
            
            # Don't recalculate finish time if we're on the last video (looping)
            # Skip detection on the final video doesn't change rotation timing
            video_files = self._get_video_files_in_order()
            new_finish_time = self.original_finish_time  # Default to original
            
            if not (video_files and len(video_files) <= 1):
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
            else:
                logger.info("Skipping finish time recalculation - last video is looping, will use original finish time")
            
            # Update tracking
            self.last_known_playback_position_ms = current_position_ms
            self.last_playback_check_time = time.time()
            
            skip_info = {
                "time_skipped_seconds": time_skipped_seconds,
                "new_finish_time": new_finish_time,
                "new_finish_time_str": new_finish_time.strftime('%H:%M:%S') if new_finish_time else "N/A",
                "current_video_filename": self.get_current_video_filename()
            }
            
            return True, skip_info
        
        # No skip, update tracking
        self.last_known_playback_position_ms = current_position_ms
        self.last_playback_check_time = time.time()
        
        # Decrement grace period on normal playback to allow it to expire
        if self._grace_period_checks > 0:
            self._grace_period_checks -= 1
        
        return False, None
