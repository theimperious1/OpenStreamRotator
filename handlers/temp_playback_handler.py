"""
Temp Playback Handler - Handles temporary playback mode during long downloads.

When downloads take longer than current content, this handler enables streaming
directly from the pending folder while downloads continue. Videos are deleted 
after playing, and archive.txt prevents re-downloading deleted videos.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Callable, TYPE_CHECKING

from config.config_manager import ConfigManager
from core.database import DatabaseManager
from managers.playlist_manager import PlaylistManager

if TYPE_CHECKING:
    from controllers.obs_controller import OBSController
    from managers.stream_manager import StreamManager
    from playback.playback_skip_detector import PlaybackSkipDetector

logger = logging.getLogger(__name__)

# OBS VLC source name - can be overridden via environment variable
VLC_SOURCE_NAME = os.getenv("VLC_SOURCE_NAME", "Playlist")


class TempPlaybackHandler:
    """Handles temporary playback mode during long playlist downloads."""

    def __init__(
        self,
        db: DatabaseManager,
        config: ConfigManager,
        playlist_manager: PlaylistManager,
        obs_controller: 'OBSController',
        stream_manager: 'StreamManager'
    ):
        self.db = db
        self.config = config
        self.playlist_manager = playlist_manager
        self.obs_controller = obs_controller
        self.stream_manager = stream_manager
        
        # State
        self._active = False
        self._override_queued = False
        self._last_folder_check = 0
        
        # External references (set by automation controller)
        self.playback_skip_detector: Optional['PlaybackSkipDetector'] = None
        self.current_session_id: Optional[int] = None
        
        # Callbacks for automation controller coordination
        self._check_manual_override_callback: Optional[Callable] = None
        self._auto_resume_downloads_callback: Optional[Callable] = None
        self._initialize_skip_detector_callback: Optional[Callable] = None
        self._set_override_prep_ready_callback: Optional[Callable[[bool], None]] = None
        
        # Reference to background download flag (shared with automation controller)
        self._get_background_download_in_progress: Optional[Callable[[], bool]] = None
        self._set_background_download_in_progress: Optional[Callable[[bool], None]] = None

    def set_skip_detector(self, detector: 'PlaybackSkipDetector') -> None:
        """Set the playback skip detector reference."""
        self.playback_skip_detector = detector

    def set_session_id(self, session_id: Optional[int]) -> None:
        """Update the current session ID."""
        self.current_session_id = session_id

    def set_callbacks(
        self,
        check_manual_override: Optional[Callable] = None,
        auto_resume_downloads: Optional[Callable] = None,
        initialize_skip_detector: Optional[Callable] = None,
        get_background_download_in_progress: Optional[Callable[[], bool]] = None,
        set_background_download_in_progress: Optional[Callable[[bool], None]] = None,
        set_override_prep_ready: Optional[Callable[[bool], None]] = None
    ) -> None:
        """Set callbacks for coordination with automation controller."""
        self._check_manual_override_callback = check_manual_override
        self._auto_resume_downloads_callback = auto_resume_downloads
        self._initialize_skip_detector_callback = initialize_skip_detector
        self._get_background_download_in_progress = get_background_download_in_progress
        self._set_background_download_in_progress = set_background_download_in_progress
        self._set_override_prep_ready_callback = set_override_prep_ready

    @property
    def is_active(self) -> bool:
        """Check if temp playback is currently active."""
        return self._active

    @property
    def override_queued(self) -> bool:
        """Check if an override is queued to run after temp playback exits."""
        return self._override_queued

    def queue_override(self) -> None:
        """Queue an override to execute after temp playback exits."""
        self._override_queued = True
        logger.info("Override queued to run after temp playback exits")

    def _is_background_download_in_progress(self) -> bool:
        """Check if background download is in progress."""
        if self._get_background_download_in_progress:
            return self._get_background_download_in_progress()
        return False

    def _set_background_download_flag(self, value: bool) -> None:
        """Set the background download in progress flag."""
        if self._set_background_download_in_progress:
            self._set_background_download_in_progress(value)

    async def activate(self, session: dict) -> None:
        """Activate temporary playback while large playlist downloads complete.
        
        Scenario: Current rotation finished but next large playlist (e.g., 28 videos)
        still downloading. Point OBS directly at pending folder to stream completed videos
        while downloads continue. Videos are deleted after playing (handled by skip detector).
        The archive.txt file ensures yt-dlp won't re-download deleted videos.
        Once all downloads complete, do normal rotation: nuke live, move pending to live.
        """
        logger.info("===== TEMP PLAYBACK ACTIVATION =====")
        
        settings = self.config.get_settings()
        pending_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
        
        # Switch to content-switch scene briefly for VLC source update
        if not self.obs_controller or not self.obs_controller.switch_scene('content-switch'):
            logger.error("Failed to switch to content-switch scene for temp playback setup")
            return
        
        await asyncio.sleep(1.5)  # Wait for scene switch
        
        try:
            # Get complete video files from pending folder
            complete_files = self.playlist_manager.get_complete_video_files(pending_folder)
            
            if not complete_files:
                logger.error("No complete files found in pending folder, cannot activate temp playback")
                return
            
            # Sort files to ensure consistent order with how VLC will play them
            # (sorted alphabetically, same as update_vlc_source does internally)
            complete_files = sorted(complete_files)
            
            # Point OBS VLC source directly at pending folder (no copying needed)
            # archive.txt ensures yt-dlp won't re-download videos deleted during playback
            if not self.obs_controller:
                logger.error("No OBS controller available")
                return
            
            success, playlist = self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, pending_folder, playlist=complete_files)
            if not success:
                logger.error("Failed to update VLC source to pending folder")
                return
            
            # Switch back to Stream scene to resume streaming
            await asyncio.sleep(0.5)
            if not self.obs_controller.switch_scene('Stream'):
                logger.error("Failed to switch back to Stream scene after temp playback setup")
                return
            
            # Mark temp playback as active
            self._active = True
            self._last_folder_check = time.time()
            
            # Update skip detector to track files in pending folder
            # This allows videos to be deleted after they finish playing
            if self.playback_skip_detector:
                self.playback_skip_detector.video_folder = pending_folder
                # Reset skip detector state so it tracks fresh from pending folder
                self.playback_skip_detector.reset()
                # Set the VLC playlist for reliable video tracking
                self.playback_skip_detector.set_vlc_playlist(playlist)
                # Enable temp playback mode with refresh callback
                self.playback_skip_detector.set_temp_playback_mode(True, self.refresh_vlc)
                # Set position change callback for crash recovery persistence
                session_id = self.current_session_id
                self.playback_skip_detector.set_position_change_callback(
                    lambda pos, sid=session_id: self.db.update_temp_playback_position(sid, pos) if sid else None
                )
                logger.info(f"Updated skip detector with {len(playlist)} video playlist")
            
            # Update stream title to reflect temp playback content
            # The next_playlists column contains the prepared rotation playlists
            if session and session.get('next_playlists'):
                try:
                    next_playlist_names = json.loads(session.get('next_playlists', '[]'))
                    if next_playlist_names:
                        new_title = self.playlist_manager.generate_stream_title(next_playlist_names)
                        if self.stream_manager:
                            await self.stream_manager.update_title(new_title)
                        logger.info(f"Updated stream title for temp playback: {new_title}")
                        
                        # Determine category from first video's playlist
                        # Since videos haven't been registered in DB yet, we'll identify
                        # which next_playlist each file belongs to by checking the database
                        # or falling back to sequential assignment based on download order
                        category = None
                        
                        if complete_files:
                            # Try to get category from first video's playlist metadata
                            first_video = complete_files[0]
                            # Check each next_playlist to see which one this video likely belongs to
                            # by checking if we can find it in the database
                            playlists_config = self.config.get_playlists()
                            
                            # Try database lookup first (may work if videos were pre-registered)
                            try:
                                video_data = self.db.get_video_by_filename(first_video)
                                if video_data and video_data.get('playlist_name'):
                                    playlist_name = video_data.get('playlist_name')
                                    for p in playlists_config:
                                        if p.get('name') == playlist_name:
                                            category = p.get('category') or p.get('name')
                                            logger.info(f"Got category from first video DB lookup: {first_video} -> {category}")
                                            break
                            except Exception as e:
                                logger.debug(f"First video not in DB yet (expected during active download): {e}")
                        
                        # Fallback: if first video not in DB, use first next_playlist
                        # This assumes videos are downloaded in order from next_playlists
                        if not category and next_playlist_names:
                            try:
                                playlists_config = self.config.get_playlists()
                                for p in playlists_config:
                                    if p.get('name') == next_playlist_names[0]:
                                        category = p.get('category') or p.get('name')
                                        logger.info(f"Using category from first next_playlist ({next_playlist_names[0]}): {category}")
                                        break
                            except Exception as e:
                                logger.warning(f"Failed to get category from next_playlists: {e}")
                        
                        # Update category if determined
                        if category and self.stream_manager:
                            await self.stream_manager.update_stream_info(new_title, category)
                            logger.info(f"Updated stream category for temp playback: {category}")
                except Exception as e:
                    logger.warning(f"Failed to update stream title/category during temp playback: {e}")
            
            # Save temp playback state for crash recovery
            if self.current_session_id:
                self.db.save_temp_playback_state(
                    self.current_session_id,
                    playlist,
                    0,  # Starting at position 0
                    pending_folder
                )
            
            logger.info(f"Temp playback activated with {len(complete_files)} files")
            logger.info(f"Streaming directly from pending folder: {pending_folder}")
            logger.info("Videos will be deleted after playing, archive.txt prevents re-download")
            
        except Exception as e:
            logger.error(f"Error during temp playback activation: {e}")
            # Switch back to Stream scene on error
            try:
                await asyncio.sleep(0.5)
                if self.obs_controller:
                    self.obs_controller.switch_scene('Stream')
            except Exception as scene_error:
                logger.error(f"Failed to recover scene after temp playback error: {scene_error}")

    async def restore(self, session: dict, temp_state: dict) -> bool:
        """Restore temp playback after a crash/restart.
        
        Args:
            session: The current session from database
            temp_state: Temp playback state dict with 'playlist', 'position', 'folder', 'cursor_ms'
        
        Returns:
            True if successfully restored, False otherwise
        """
        logger.info("===== RESTORING TEMP PLAYBACK FROM CRASH =====")
        
        try:
            saved_playlist = temp_state.get('playlist', [])
            saved_position = temp_state.get('position', 0)
            pending_folder = temp_state.get('folder')
            saved_cursor_ms = temp_state.get('cursor_ms', 0)
            
            if not pending_folder or not saved_playlist:
                logger.error("Invalid temp playback state - missing folder or playlist")
                return False
            
            # Validate that remaining files actually exist
            remaining_playlist = saved_playlist[saved_position:]
            valid_playlist = []
            
            for filename in remaining_playlist:
                file_path = os.path.join(pending_folder, filename)
                if os.path.exists(file_path):
                    valid_playlist.append(filename)
                else:
                    logger.warning(f"Skipping missing file during temp playback restore: {filename}")
            
            if not valid_playlist:
                logger.error("No valid files remaining for temp playback restore")
                return False
            
            logger.info(f"Restoring temp playback: {len(valid_playlist)} valid files from position {saved_position}")
            
            # Switch to content-switch scene briefly for VLC source update
            if not self.obs_controller or not self.obs_controller.switch_scene('content-switch'):
                logger.error("Failed to switch to content-switch scene for temp playback restore")
                return False
            
            await asyncio.sleep(1.5)
            
            # Update OBS VLC source with valid remaining playlist
            success, playlist = self.obs_controller.update_vlc_source(
                VLC_SOURCE_NAME, 
                pending_folder, 
                playlist=valid_playlist
            )
            if not success:
                logger.error("Failed to update VLC source during temp playback restore")
                return False
            
            # Switch back to Stream scene
            await asyncio.sleep(0.5)
            if not self.obs_controller.switch_scene('Stream'):
                logger.error("Failed to switch back to Stream scene after temp playback restore")
                return False
            
            # Seek to saved cursor position if we have one
            if saved_cursor_ms > 0 and self.obs_controller:
                await asyncio.sleep(0.5)  # Give VLC time to start playing
                seek_success = self.obs_controller.seek_media(VLC_SOURCE_NAME, saved_cursor_ms)
                if seek_success:
                    logger.info(f"Seeked to saved cursor position: {saved_cursor_ms}ms ({saved_cursor_ms/1000:.1f}s)")
                else:
                    logger.warning(f"Failed to seek to saved cursor position: {saved_cursor_ms}ms")
            
            # Mark temp playback as active
            self._active = True
            self._last_folder_check = time.time()
            # Set background download flag - if we're in temp playback, downloads haven't finished
            self._set_background_download_flag(True)
            
            # Initialize the skip detector (creates it if None, initializes state)
            if self._initialize_skip_detector_callback:
                self._initialize_skip_detector_callback()
            
            # Configure skip detector for temp playback mode
            if self.playback_skip_detector:
                self.playback_skip_detector.video_folder = pending_folder
                self.playback_skip_detector.reset()
                # Set playlist but position starts at 0 since we rebuilt the valid playlist
                self.playback_skip_detector.set_vlc_playlist(valid_playlist)
                self.playback_skip_detector.set_temp_playback_mode(True, self.refresh_vlc)
                # Set position change callback for crash recovery persistence
                session_id = self.current_session_id
                self.playback_skip_detector.set_position_change_callback(
                    lambda pos, sid=session_id: self.db.update_temp_playback_position(sid, pos) if sid else None
                )
                logger.info(f"Configured skip detector for temp playback with {len(valid_playlist)} videos")
            
            # Update database with corrected state
            if self.current_session_id:
                self.db.save_temp_playback_state(
                    self.current_session_id,
                    valid_playlist,
                    0,  # Reset to 0 since we rebuilt playlist from remaining files
                    pending_folder
                )
            
            # Update stream title from next_playlists
            if session and session.get('next_playlists'):
                try:
                    next_playlist_names = json.loads(session.get('next_playlists', '[]'))
                    if next_playlist_names:
                        new_title = self.playlist_manager.generate_stream_title(next_playlist_names)
                        if self.stream_manager:
                            await self.stream_manager.update_title(new_title)
                        logger.info(f"Restored stream title for temp playback: {new_title}")
                except Exception as e:
                    logger.warning(f"Failed to restore stream title during temp playback restore: {e}")
            
            logger.info(f"Temp playback restored with {len(valid_playlist)} files")
            logger.info(f"Streaming from pending folder: {pending_folder}")
            
            # Resume pending downloads in background
            if session and session.get('next_playlists_status') and self.current_session_id:
                try:
                    status_dict = json.loads(session.get('next_playlists_status', '{}'))
                    # Find playlists with PENDING status
                    pending_playlists = [name for name, status in status_dict.items() if status == "PENDING"]
                    if pending_playlists:
                        logger.info(f"Resuming {len(pending_playlists)} pending downloads after temp playback restore")
                        if self._auto_resume_downloads_callback:
                            await self._auto_resume_downloads_callback(
                                self.current_session_id, pending_playlists, status_dict
                            )
                    else:
                        logger.info("All playlists already downloaded (no PENDING status found)")
                        # All downloads complete - let monitor exit temp playback normally
                        self._set_background_download_flag(False)
                except Exception as e:
                    logger.warning(f"Failed to resume pending downloads after temp playback restore: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during temp playback restore: {e}")
            # Try to switch back to Stream scene
            try:
                await asyncio.sleep(0.5)
                if self.obs_controller:
                    self.obs_controller.switch_scene('Stream')
            except Exception as scene_error:
                logger.error(f"Failed to recover scene after temp playback restore error: {scene_error}")
            return False

    async def refresh_vlc(self) -> None:
        """Refresh VLC source during temp playback when playlist is exhausted but new files available.
        
        This is called by the skip detector when:
        1. We're in temp playback mode
        2. The current video is the last one in the tracked VLC playlist
        3. There are new files in the pending folder (from ongoing downloads)
        
        We briefly switch to content-switch scene, refresh VLC, then switch back.
        """
        logger.info("===== VLC REFRESH DURING TEMP PLAYBACK =====")
        
        if not self._active:
            logger.warning("VLC refresh called but temp playback not active")
            return
        
        settings = self.config.get_settings()
        pending_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
        
        try:
            # Switch to content-switch scene briefly
            if not self.obs_controller or not self.obs_controller.switch_scene('content-switch'):
                logger.error("Failed to switch to content-switch scene for VLC refresh")
                return
            
            await asyncio.sleep(1.0)  # Wait for scene switch
            
            # Update VLC source with current pending folder contents
            success, playlist = self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, pending_folder)
            if not success:
                logger.error("Failed to refresh VLC source")
                return
            
            # Switch back to Stream scene
            await asyncio.sleep(0.3)
            if not self.obs_controller.switch_scene('Stream'):
                logger.error("Failed to switch back to Stream scene after VLC refresh")
                return
            
            # Update skip detector with new playlist
            if self.playback_skip_detector:
                self.playback_skip_detector.set_vlc_playlist(playlist)
                logger.info(f"Refreshed VLC with {len(playlist)} videos during temp playback")
            
        except Exception as e:
            logger.error(f"Error during VLC refresh: {e}")
            # Try to switch back to Stream scene
            try:
                if self.obs_controller:
                    self.obs_controller.switch_scene('Stream')
            except:
                pass

    async def monitor(self) -> None:
        """Monitor pending folder during temp playback.
        
        Since we're streaming directly from pending folder, no file copying is needed.
        Just check if the background download task has completed and trigger rotation.
        New files are automatically picked up by VLC as they complete downloading.
        
        NOTE: Guard check ensures this doesn't trigger if flag is already cleared
        (e.g., by execute_content_switch() completing a normal rotation).
        """
        # Guard: Don't process if temp playback is no longer active
        # (normal rotation may have completed and cleared the flag)
        if not self._active:
            return
        
        try:
            # Check if background download is still running
            # This is more reliable than checking for .part files, which may have
            # brief gaps between videos
            if not self._is_background_download_in_progress():
                logger.info("Background download completed, exiting temp playback")
                await self.exit()
                
        except Exception as e:
            logger.error(f"Error monitoring temp playback: {e}")

    async def exit(self) -> None:
        """Exit temp playback mode and do normal rotation: nuke live, move pending to live.
        
        Since we stream directly from pending during temp playback, this is essentially
        a normal rotation. archive.txt is excluded from move and deleted after.
        """
        logger.info("===== TEMP PLAYBACK EXIT =====")
        
        try:
            settings = self.config.get_settings()
            
            # Switch to content-switch scene for folder operations
            if not self.obs_controller or not self.obs_controller.switch_scene('content-switch'):
                logger.error("Failed to switch to content-switch scene for temp playback exit")
                return
            
            await asyncio.sleep(1.5)
            
            pending_folder = settings.get('next_rotation_folder', 'C:/stream_videos_next/')
            live_folder = settings.get('video_folder', 'C:/stream_videos/')
            
            # Use the standard folder switch which handles archive.txt exclusion and deletion
            if not self.playlist_manager.switch_content_folders(live_folder, pending_folder):
                logger.error("Failed to switch content folders during temp playback exit")
                return
            
            # Update OBS to stream from live folder
            await asyncio.sleep(0.5)
            if not self.obs_controller:
                logger.error("No OBS controller available")
                return
            
            success, playlist = self.obs_controller.update_vlc_source(VLC_SOURCE_NAME, live_folder)
            if not success:
                logger.error("Failed to update VLC source to live folder")
                return
            
            # Switch back to Stream scene
            await asyncio.sleep(0.5)
            if not self.obs_controller.switch_scene('Stream'):
                logger.error("Failed to switch back to Stream scene after temp playback exit")
                return
            
            # Clear temp playback state
            self._active = False
            
            # Clear temp playback state from database (crash recovery no longer needed)
            if self.current_session_id:
                self.db.clear_temp_playback_state(self.current_session_id)
            
            # Update skip detector to track files in live folder
            if self.playback_skip_detector:
                self.playback_skip_detector.video_folder = live_folder
                # Disable temp playback mode
                self.playback_skip_detector.set_temp_playback_mode(False)
                # Set the new VLC playlist while preserving current playback position
                # (reset_position=False because VLC is already mid-rotation, we're just updating
                # the playlist list while maintaining the same position in the stream)
                self.playback_skip_detector.set_vlc_playlist(playlist, reset_position=False)
                logger.info(f"Updated skip detector to track live folder with {len(playlist)} videos")
            
            # Recalculate estimated finish time based on new content in live folder
            # This accounts for what was already consumed during temp playback
            if self.current_session_id and self.playback_skip_detector:
                try:
                    # Get total duration of all videos now in live folder
                    total_duration_seconds = 0
                    for filename in playlist:
                        video = self.db.get_video_by_filename(filename)
                        if video and video.get('duration_seconds'):
                            total_duration_seconds += video['duration_seconds']
                    
                    # Get cumulative playback so far (includes temp playback consumption)
                    cumulative_playback_ms = self.playback_skip_detector.cumulative_playback_ms
                    cumulative_playback_seconds = cumulative_playback_ms / 1000
                    
                    # Calculate remaining duration
                    remaining_seconds = max(0, total_duration_seconds - cumulative_playback_seconds)
                    
                    # Calculate new finish time
                    current_time = datetime.now()
                    new_finish_time = current_time + timedelta(seconds=remaining_seconds)
                    
                    logger.info(
                        f"Recalculating finish time after temp playback exit: "
                        f"total={total_duration_seconds}s, consumed={cumulative_playback_seconds:.1f}s, "
                        f"remaining={remaining_seconds:.1f}s"
                    )
                    logger.info(f"New estimated finish time: {new_finish_time}")
                    
                    # Update session with new finish time
                    self.db.update_session_times(
                        self.current_session_id,
                        new_finish_time.isoformat(),
                        (new_finish_time - timedelta(minutes=30)).isoformat()
                    )
                    
                    # Re-initialize skip detector with new duration and finish time
                    self.playback_skip_detector.initialize(
                        total_duration_seconds=int(remaining_seconds),
                        original_finish_time=new_finish_time
                    )
                    
                except Exception as e:
                    logger.error(f"Error recalculating finish time after temp playback exit: {e}")
            
            # Check if override was queued during temp playback
            if self._override_queued:
                logger.info("Override was queued during temp playback - executing it now")
                self._override_queued = False
                # Set override prep ready flag so automation controller can commit Phase 2
                if self._set_override_prep_ready_callback:
                    self._set_override_prep_ready_callback(True)
                if self._check_manual_override_callback:
                    await self._check_manual_override_callback()
            
            logger.info("Temp playback successfully exited, resuming normal rotation cycle")
            
        except Exception as e:
            logger.error(f"Error during temp playback exit: {e}")
            try:
                await asyncio.sleep(0.5)
                if self.obs_controller:
                    self.obs_controller.switch_scene('Stream')
            except Exception as scene_error:
                logger.error(f"Failed to recover scene after temp playback exit error: {scene_error}")

    async def cleanup_after_rotation(self) -> None:
        """Clean up temp playback after normal rotation completes.
        
        When a normal rotation completes while temp playback is active (streaming
        directly from pending), the rotation has already moved files from pending → live.
        This method handles:
        1. Update skip detector back to live folder
        2. Execute any queued overrides
        3. Clear temp playback flag
        """
        logger.info("Cleaning up temp playback after normal rotation")
        
        try:
            settings = self.config.get_settings()
            video_folder = settings.get('video_folder', 'C:/stream_videos/')
            
            # Step 1: Update skip detector to track live folder
            if self.playback_skip_detector:
                self.playback_skip_detector.video_folder = video_folder
                logger.info("Updated skip detector to track live folder")
            
            # Step 2: Execute queued override if one was triggered during temp playback
            if self._override_queued:
                logger.info("Override was queued during temp playback - executing it now")
                self._override_queued = False
                if self._check_manual_override_callback:
                    await self._check_manual_override_callback()
            
            # Step 3: Clear temp playback flag
            self._active = False
            logger.info("Temp playback cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during temp playback cleanup: {e}")
            # Ensure flag is cleared even on error
            self._active = False
