"""
Temp Playback Handler - Handles temporary playback mode during long downloads.

When downloads take longer than current content, this handler enables streaming
directly from the pending folder while downloads continue. Videos are deleted 
after playing, and archive.txt prevents re-downloading deleted videos.
"""

import asyncio
import logging
import os
import time
from typing import Optional, Callable, TYPE_CHECKING
from utils.video_utils import strip_ordering_prefix
from config.constants import VIDEO_EXTENSIONS
from config.config_manager import ConfigManager
from core.database import DatabaseManager
from managers.playlist_manager import PlaylistManager
from services.notification_service import NotificationService
from config.constants import DEFAULT_NEXT_ROTATION_FOLDER, DEFAULT_VIDEO_FOLDER
from utils.video_utils import resolve_playlist_categories

if TYPE_CHECKING:
    from controllers.obs_controller import OBSController
    from managers.stream_manager import StreamManager

logger = logging.getLogger(__name__)


class TempPlaybackHandler:

    def __init__(
        self,
        db: DatabaseManager,
        config: ConfigManager,
        playlist_manager: PlaylistManager,
        obs_controller: 'OBSController',
        stream_manager: 'StreamManager',
        notification_service: Optional[NotificationService] = None,
        scene_stream: str = "OSR Stream",
        scene_rotation_screen: str = "OSR Rotation screen",
        vlc_source_name: str = "OSR Playlist"
    ):
        self.db = db
        self.config = config
        self.playlist_manager = playlist_manager
        self.obs_controller = obs_controller
        self.stream_manager = stream_manager
        self.notification_service = notification_service
        self.scene_stream = scene_stream
        self.scene_rotation_screen = scene_rotation_screen
        self.vlc_source_name = vlc_source_name
        
        # State
        self._active = False
        self._last_folder_check = 0
        
        # External references (set by automation controller)
        self.current_session_id: Optional[int] = None
        
        # Callbacks for automation controller coordination
        self._auto_resume_downloads_callback: Optional[Callable] = None
        self._trigger_next_rotation_callback: Optional[Callable] = None
        self._reinitialize_playback_monitor_callback: Optional[Callable] = None
        self._update_category_after_switch_callback: Optional[Callable] = None
        self._set_pending_seek_callback: Optional[Callable] = None
        
        # Reference to background download flag (shared with automation controller)
        self._get_background_download_in_progress: Optional[Callable[[], bool]] = None
        self._set_background_download_in_progress: Optional[Callable[[bool], None]] = None

    def set_session_id(self, session_id: Optional[int]) -> None:
        """Update the current session ID."""
        self.current_session_id = session_id

    def set_callbacks(
        self,
        auto_resume_downloads: Optional[Callable] = None,
        get_background_download_in_progress: Optional[Callable[[], bool]] = None,
        set_background_download_in_progress: Optional[Callable[[bool], None]] = None,
        trigger_next_rotation: Optional[Callable] = None,
        reinitialize_playback_monitor: Optional[Callable] = None,
        update_category_after_switch: Optional[Callable] = None,
        set_pending_seek: Optional[Callable] = None,
    ) -> None:
        """Set callbacks for coordination with automation controller."""
        self._auto_resume_downloads_callback = auto_resume_downloads
        self._get_background_download_in_progress = get_background_download_in_progress
        self._set_background_download_in_progress = set_background_download_in_progress
        self._trigger_next_rotation_callback = trigger_next_rotation
        self._reinitialize_playback_monitor_callback = reinitialize_playback_monitor
        self._update_category_after_switch_callback = update_category_after_switch
        self._set_pending_seek_callback = set_pending_seek

    @property
    def is_active(self) -> bool:
        """Check if temp playback is currently active."""
        return self._active

    def _is_background_download_in_progress(self) -> bool:
        """Check if background download is in progress."""
        if self._get_background_download_in_progress:
            return self._get_background_download_in_progress()
        return False

    def _set_background_download_flag(self, value: bool) -> None:
        """Set the background download in progress flag."""
        if self._set_background_download_in_progress:
            self._set_background_download_in_progress(value)

    async def activate(self, session: dict) -> bool:
        """Activate temporary playback while large playlist downloads complete.
        
        Scenario: Current rotation finished but next large playlist (e.g., 28 videos)
        still downloading. Point OBS directly at pending folder to stream completed videos
        while downloads continue. Videos are deleted after playing (handled by skip detector).
        The archive.txt file ensures yt-dlp won't re-download deleted videos.
        Once all downloads complete, do normal rotation: nuke live, move pending to live.
        
        If no complete files are available yet, polls every few seconds until content
        appears (up to 2 minutes). The rotation screen is shown while waiting so the
        stream isn't dead.
        
        Returns:
            True if temp playback was successfully activated, False otherwise.
        """
        logger.info("===== TEMP PLAYBACK ACTIVATION =====")
        
        settings = self.config.get_settings()
        pending_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
        
        # Switch to Rotation screen scene briefly for VLC source update
        if not self.obs_controller or not self.obs_controller.switch_scene(self.scene_rotation_screen):
            logger.error("Failed to switch to Rotation screen scene for temp playback setup")
            return False
        
        await asyncio.sleep(1.5)  # Wait for scene switch
        
        try:
            # Get complete video files from pending folder
            complete_files = self.playlist_manager.get_complete_video_files(pending_folder)
            
            if not complete_files:
                # No files ready yet — poll until downloads produce content.
                # The rotation screen is already shown so the stream isn't dead.
                logger.info("No complete files in pending folder yet — waiting for downloads to produce content...")
                max_wait = 120  # 2 minutes
                poll_interval = 5  # seconds
                waited = 0
                while waited < max_wait:
                    await asyncio.sleep(poll_interval)
                    waited += poll_interval
                    complete_files = self.playlist_manager.get_complete_video_files(pending_folder)
                    if complete_files:
                        logger.info(f"Content appeared after waiting {waited}s: {len(complete_files)} file(s) ready")
                        break
                    if waited % 30 == 0:
                        logger.info(f"Still waiting for content in pending folder... ({waited}s elapsed)")
                
                if not complete_files:
                    logger.warning(f"Timed out after {max_wait}s waiting for content — will retry on next tick")
                    # Leave the rotation screen showing so the stream isn't
                    # dead while the tick loop retries on the next iteration.
                    return False
            
            # Point OBS VLC source directly at pending folder (no copying needed)
            # archive.txt ensures yt-dlp won't re-download videos deleted during playback
            if not self.obs_controller:
                logger.error("No OBS controller available")
                return False
            
            success, playlist = self.obs_controller.update_vlc_source(self.vlc_source_name, pending_folder)
            if not success:
                logger.error("Failed to update VLC source to pending folder")
                return False
            
            # Switch back to Stream scene to resume streaming
            await asyncio.sleep(0.5)
            if not self.obs_controller.switch_scene(self.scene_stream):
                logger.error("Failed to switch back to Stream scene after temp playback setup")
                return False
            
            # Mark temp playback as active
            self._active = True
            self._last_folder_check = time.time()
            
            # Update stream title and category to reflect temp playback content.
            # Resolve category BEFORE the API call so Kick gets the correct
            # category in a single request instead of falling back to Just Chatting.
            if session and session.get('next_playlists'):
                try:
                    next_playlist_names = DatabaseManager.parse_json_field(session.get('next_playlists'), [])
                    if next_playlist_names:
                        new_title = self.playlist_manager.generate_stream_title(next_playlist_names)

                        # Determine category from first video's playlist
                        category = None

                        if complete_files:
                            # Try DB lookup first (may work if videos were pre-registered)
                            first_video = complete_files[0]
                            playlists_config = self.config.get_playlists()
                            try:
                                video_data = self.db.get_video_by_filename(first_video)
                                if video_data and video_data.get('playlist_name'):
                                    playlist_name = video_data.get('playlist_name')
                                    for p in playlists_config:
                                        if p.get('name') == playlist_name:
                                            category = resolve_playlist_categories(p)
                                            logger.info(f"Got category from first video DB lookup: {first_video} -> {category}")
                                            break
                            except Exception as e:
                                logger.debug(f"First video not in DB yet (expected during active download): {e}")

                        # Fallback: use first next_playlist's category
                        if not category and next_playlist_names:
                            try:
                                playlists_config = self.config.get_playlists()
                                for p in playlists_config:
                                    if p.get('name') == next_playlist_names[0]:
                                        category = resolve_playlist_categories(p)
                                        logger.info(f"Using category from first next_playlist ({next_playlist_names[0]}): {category}")
                                        break
                            except Exception as e:
                                logger.warning(f"Failed to get category from next_playlists: {e}")

                        # Single API call with both title + category
                        if self.stream_manager:
                            if category:
                                await self.stream_manager.update_stream_info(new_title, category)
                                logger.info(f"Updated stream title and category for temp playback: {new_title} / {category}")
                            else:
                                await self.stream_manager.update_title(new_title)
                                logger.info(f"Updated stream title for temp playback: {new_title}")
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
            
            if self.notification_service:
                self.notification_service.notify_temp_playback_activated(len(complete_files))
            
            return True
            
        except Exception as e:
            logger.error(f"Error during temp playback activation: {e}")
            # Switch back to Stream scene on error
            try:
                await asyncio.sleep(0.5)
                if self.obs_controller:
                    self.obs_controller.switch_scene(self.scene_stream)
            except Exception as scene_error:
                logger.error(f"Failed to recover scene after temp playback error: {scene_error}")
            return False

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
            
            if not pending_folder or not saved_playlist:
                logger.error("Invalid temp playback state - missing folder or playlist")
                return False
            
            # The temp_playback_cursor_ms and temp_playback_position fields
            # are NOT kept current during playback (only set at activation).
            # The REAL cursor lives in the main session fields that
            # _tick_save_playback writes every second.
            saved_video = session.get('playback_current_video')
            saved_cursor_ms = session.get('playback_cursor_ms', 0)
            if saved_video and saved_cursor_ms:
                logger.info(
                    f"Crash recovery cursor from session: "
                    f"{saved_video} at {saved_cursor_ms}ms ({saved_cursor_ms/1000:.1f}s)"
                )
            
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
            
            # If we know which video was playing, reorder the playlist so
            # VLC starts on that file (VLC always begins from index 0).
            if saved_video and saved_video in valid_playlist and saved_video != valid_playlist[0]:
                valid_playlist = [saved_video] + [f for f in valid_playlist if f != saved_video]
                logger.info(f"Reordered VLC playlist for resume: {saved_video}")
            
            logger.info(f"Restoring temp playback: {len(valid_playlist)} valid files from position {saved_position}")
            
            # Switch to Rotation screen scene briefly for VLC source update
            if not self.obs_controller or not self.obs_controller.switch_scene(self.scene_rotation_screen):
                logger.error("Failed to switch to Rotation screen scene for temp playback restore")
                return False
            
            await asyncio.sleep(1.5)
            
            # Update OBS VLC source with valid remaining playlist
            success, playlist = self.obs_controller.update_vlc_source(
                self.vlc_source_name, 
                pending_folder, 
                playlist=valid_playlist
            )
            if not success:
                logger.error("Failed to update VLC source during temp playback restore")
                return False
            
            # Switch back to Stream scene
            await asyncio.sleep(0.5)
            if not self.obs_controller.switch_scene(self.scene_stream):
                logger.error("Failed to switch back to Stream scene after temp playback restore")
                return False
            
            # NOTE: We do NOT seek here.  The cursor position is restored
            # by the caller via the deferred-seek mechanism which waits
            # for VLC to report "playing" before issuing the seek — much
            # more reliable than the old fixed-delay approach.
            
            # Mark temp playback as active
            self._active = True
            self._last_folder_check = time.time()
            # Set background download flag - if we're in temp playback, downloads haven't finished
            self._set_background_download_flag(True)
            
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
                    next_playlist_names = DatabaseManager.parse_json_field(session.get('next_playlists'), [])
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
                    status_dict: dict = DatabaseManager.parse_json_field(session.get('next_playlists_status'), {})
                    # Find playlists with PENDING status
                    pending_playlists = [name for name, status in status_dict.items() if status == "PENDING"]
                    if pending_playlists:
                        logger.info(f"Resuming {len(pending_playlists)} pending downloads after temp playback restore")
                        if self._auto_resume_downloads_callback:
                            await self._auto_resume_downloads_callback(
                                self.current_session_id, pending_playlists
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
                    self.obs_controller.switch_scene(self.scene_stream)
            except Exception as scene_error:
                logger.error(f"Failed to recover scene after temp playback restore error: {scene_error}")
            return False

    async def monitor(self) -> None:
        """Monitor pending folder during temp playback.
        
        Since we're streaming directly from pending folder, no file copying is needed.
        Just check if all prepared playlists have been marked COMPLETED in the database.
        Once all downloads complete, trigger rotation to move pending → live.
        New files are automatically picked up by VLC as they complete downloading.
        
        NOTE: Guard check ensures this doesn't trigger if flag is already cleared
        (e.g., by execute_content_switch() completing a normal rotation).
        """
        # Guard: Don't process if temp playback is no longer active
        # (normal rotation may have completed and cleared the flag)
        if not self._active:
            return
        
        try:
            # Check if all prepared playlists are marked as COMPLETED in the database
            # This is more reliable than checking flags which may be out of sync
            session = self.db.get_current_session()
            if not session or not self.current_session_id:
                return
            
            # Get the prepared playlist names and their statuses
            next_playlists = DatabaseManager.parse_json_field(session.get('next_playlists'), [])
            next_playlists_status: dict = DatabaseManager.parse_json_field(session.get('next_playlists_status'), {})
            
            # If no playlists are being prepared, nothing to wait for
            if not next_playlists:
                return
            
            # Check if all prepared playlists are COMPLETED
            all_completed = all(next_playlists_status.get(pl) == "COMPLETED" for pl in next_playlists)
            
            if all_completed:
                logger.info(f"All prepared playlists completed: {next_playlists} - exiting temp playback")
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
            
            # Capture playback position BEFORE switching scenes so we can
            # resume at the same point after VLC reloads from the live folder.
            saved_cursor_ms: Optional[int] = None
            saved_video: Optional[str] = None
            if self.obs_controller:
                try:
                    status = self.obs_controller.get_media_input_status(self.vlc_source_name)
                    if status and status.get('media_cursor') is not None:
                        saved_cursor_ms = status['media_cursor']
                        # Read from DB — _tick_save_playback keeps this current
                        if self.current_session_id:
                            session_snap = self.db.get_current_session()
                            if session_snap:
                                saved_video = session_snap.get('playback_current_video')
                        if saved_cursor_ms and saved_video:
                            logger.info(
                                f"Captured playback position for resume: "
                                f"{saved_video} at {saved_cursor_ms}ms ({saved_cursor_ms/1000:.1f}s)"
                            )
                except Exception as e:
                    logger.debug(f"Could not capture playback position before temp exit: {e}")
            
            # Switch to Rotation screen scene for folder operations
            if not self.obs_controller or not self.obs_controller.switch_scene(self.scene_rotation_screen):
                logger.error("Failed to switch to Rotation screen scene for temp playback exit")
                return
            
            await asyncio.sleep(1.5)
            
            pending_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
            live_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)
            
            # Use the standard folder switch which handles archive.txt exclusion and deletion
            if not self.playlist_manager.switch_content_folders(live_folder, pending_folder):
                logger.error("Failed to switch content folders during temp playback exit")
                return
            
            # Rename videos with playlist ordering prefix (01_, 02_, etc.)
            # Use next_playlists (the temp playback content) not playlists_selected (the original rotation)
            next_playlist_names = []
            try:
                session = self.db.get_current_session()
                if session:
                    next_playlist_names = DatabaseManager.parse_json_field(session.get('next_playlists'), [])
                    if next_playlist_names:
                        self.playlist_manager.rename_videos_with_playlist_prefix(live_folder, next_playlist_names)
            except Exception as e:
                logger.warning(f"Failed to rename videos with prefix during temp playback exit: {e}")
            
            # Filter to only playlists that still have files in the live folder.
            # Temp playback deletes videos after playing, so consumed playlists
            # (like MEWGENICS when all its videos were played) won't have files.
            if next_playlist_names:
                try:
                    prefix_to_playlist = {f"{i+1:02d}": name for i, name in enumerate(next_playlist_names)}
                    found_prefixes = set()
                    for filename in os.listdir(live_folder):
                        if (filename.lower().endswith(VIDEO_EXTENSIONS)
                                and len(filename) > 3
                                and filename[2] == '_'
                                and filename[:2].isdigit()):
                            found_prefixes.add(filename[:2])
                    
                    active_names = [name for name in next_playlist_names
                                    if f"{next_playlist_names.index(name)+1:02d}" in found_prefixes]
                    if active_names and active_names != next_playlist_names:
                        removed = set(next_playlist_names) - set(active_names)
                        logger.info(
                            f"Filtered consumed playlists from title: "
                            f"removed {sorted(removed)}, keeping {active_names}"
                        )
                        next_playlist_names = active_names
                except OSError as e:
                    logger.debug(f"Could not scan live folder for playlist filtering: {e}")
            
            # Update stream title to reflect the new content (CATS|MW2 instead of MUSIC|NARWHALS)
            if next_playlist_names:
                new_title = self.playlist_manager.generate_stream_title(next_playlist_names)
                try:
                    await self.stream_manager.update_title(new_title)
                    # Keep DB in sync so future category updates don't revert the title
                    if self.current_session_id:
                        self.db.update_session_stream_title(self.current_session_id, new_title)
                    logger.info(f"Updated stream title after temp playback exit: {new_title}")
                except Exception as e:
                    logger.warning(f"Failed to update stream title after temp playback exit: {e}")

                # Also update playlists_selected so config-change title regeneration
                # uses the actual playing playlists, not the original session playlists.
                try:
                    if self.current_session_id:
                        playlist_objects = self.db.get_playlists_with_ids_by_names(next_playlist_names)
                        if playlist_objects:
                            playlist_ids = [p['id'] for p in playlist_objects]
                            self.db.update_session_playlists_selected(self.current_session_id, playlist_ids)
                            logger.info(f"Updated playlists_selected to match temp playback content: {next_playlist_names}")
                except Exception as e:
                    logger.warning(f"Failed to update playlists_selected after temp playback exit: {e}")
            
            # Update OBS to stream from live folder.
            # If we captured a playback position, reorder the playlist so the
            # video we were watching is first — VLC always starts from index 0,
            # and the deferred seek only applies when the video name matches.
            await asyncio.sleep(0.5)
            if not self.obs_controller:
                logger.error("No OBS controller available")
                return
            
            resume_playlist = None
            if saved_video:
                try:
                    all_files = sorted(
                        f for f in os.listdir(live_folder)
                        if f.lower().endswith(VIDEO_EXTENSIONS)
                    )
                    # Find the prefixed filename that matches the saved original name
                    resume_file = None
                    for f in all_files:
                        if strip_ordering_prefix(f) == saved_video:
                            resume_file = f
                            break
                    if resume_file and resume_file != all_files[0]:
                        # Move the resume file to the front; keep the rest in order
                        reordered = [resume_file] + [f for f in all_files if f != resume_file]
                        resume_playlist = reordered
                        logger.info(f"Reordered VLC playlist to resume from: {resume_file}")
                except Exception as e:
                    logger.debug(f"Could not reorder playlist for resume: {e}")
            
            success, playlist = self.obs_controller.update_vlc_source(
                self.vlc_source_name, live_folder, playlist=resume_playlist
            )
            if not success:
                logger.error("Failed to update VLC source to live folder")
                return
            
            # Switch back to Stream scene
            await asyncio.sleep(0.5)
            if not self.obs_controller.switch_scene(self.scene_stream):
                logger.error("Failed to switch back to Stream scene after temp playback exit")
                return
            
            # Clear temp playback state
            self._active = False
            
            # Clear temp playback state from database (crash recovery no longer needed)
            if self.current_session_id:
                self.db.clear_temp_playback_state(self.current_session_id)
            
            # Re-initialize playback monitor to watch the live folder.
            # If the VLC playlist was reordered for resume, pass the resume
            # file so the monitor's current-video pointer matches what VLC
            # is actually playing (instead of the alphabetically first file).
            resume_file_for_monitor = resume_playlist[0] if resume_playlist else None
            if self._reinitialize_playback_monitor_callback:
                self._reinitialize_playback_monitor_callback(live_folder, resume_file_for_monitor)
            
            # Restore playback position — VLC reloaded from scratch so the
            # cursor is at 0.  Use the deferred-seek mechanism to jump back
            # to where we left off once VLC reports "playing".
            if saved_cursor_ms and saved_video and saved_cursor_ms > 0:
                if self._set_pending_seek_callback:
                    self._set_pending_seek_callback(saved_cursor_ms, saved_video)
                    logger.info(
                        f"Pending seek after temp playback exit: "
                        f"{saved_video} at {saved_cursor_ms}ms ({saved_cursor_ms/1000:.1f}s)"
                    )
            
            # Update category based on the actual video now playing
            if self._update_category_after_switch_callback:
                await self._update_category_after_switch_callback()
            
            # Trigger next rotation selection and background download
            # This ensures the automation controller prepares the playlists after this rotation finishes
            logger.info("Triggering next rotation preparation after temp playback exit")
            if self._trigger_next_rotation_callback:
                await self._trigger_next_rotation_callback()
            
            logger.info("Temp playback successfully exited, resuming normal rotation cycle")
            
            if self.notification_service and next_playlist_names:
                self.notification_service.notify_temp_playback_exited(next_playlist_names)
            
        except Exception as e:
            logger.error(f"Error during temp playback exit: {e}")
            try:
                await asyncio.sleep(0.5)
                if self.obs_controller:
                    self.obs_controller.switch_scene(self.scene_stream)
            except Exception as scene_error:
                logger.error(f"Failed to recover scene after temp playback exit error: {scene_error}")

    async def cleanup_after_rotation(self) -> None:
        """Clean up temp playback after normal rotation completes.
        
        When a normal rotation completes while temp playback is active (streaming
        directly from pending), the rotation has already moved files from pending → live.
        This method handles:
        1. Update skip detector back to live folder
        2. Clear temp playback flag
        3. Clear temp playback state from database (prevents stale crash recovery)
        """
        logger.info("Cleaning up temp playback after normal rotation")
        
        try:
            # Trigger next rotation
            logger.info("Triggering next rotation after temp playback cleanup")
            if self._trigger_next_rotation_callback:
                await self._trigger_next_rotation_callback()
            
            # Clear temp playback flag
            self._active = False
            
            # Clear temp playback state from database (crash recovery no longer needed)
            if self.current_session_id:
                self.db.clear_temp_playback_state(self.current_session_id)
            
            logger.info("Temp playback cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during temp playback cleanup: {e}")
            # Ensure flag is cleared even on error
            self._active = False
            if self.current_session_id:
                try:
                    self.db.clear_temp_playback_state(self.current_session_id)
                except Exception:
                    pass
