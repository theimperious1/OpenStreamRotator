import json
import logging
import time
from typing import Optional
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from managers.playlist_manager import PlaylistManager
from controllers.obs_controller import OBSController
from services.notification_service import NotificationService
import asyncio

logger = logging.getLogger(__name__)


class ContentSwitchHandler:
    """Handles content switching operations (normal rotations and overrides)."""

    MAX_TITLE_LENGTH = 140  # Kick's title character limit

    def __init__(self, db: DatabaseManager, config: ConfigManager,
                 playlist_manager: PlaylistManager, obs_controller: OBSController,
                 notification_service: NotificationService):
        """
        Initialize content switch handler.
        
        Args:
            db: DatabaseManager instance
            config: ConfigManager instance
            playlist_manager: PlaylistManager instance
            obs_controller: OBSController instance
            notification_service: NotificationService instance
        """
        self.db = db
        self.config = config
        self.playlist_manager = playlist_manager
        self.obs_controller = obs_controller
        self.notification_service = notification_service
        self._last_category_update_time = 0  # Track last category update to throttle spam

    def get_category_for_video(self, video_filename: str) -> Optional[str]:
        """
        Get the stream category for a specific video based on its source playlist.
        
        Args:
            video_filename: Filename of the video to look up
            
        Returns:
            Category name, or None if not found
        """
        if not video_filename:
            return None
        
        try:
            # Look up the video in database to find its source playlist
            video = self.db.get_video_by_filename(video_filename)
            if not video:
                logger.debug(f"Video not found in database: {video_filename}")
                return None
            
            playlist_name = video.get('playlist_name')
            if not playlist_name:
                logger.debug(f"No playlist_name for video: {video_filename}")
                return None
            
            # Get the category for this playlist from playlists config
            playlists_config = self.config.get_playlists()
            for p in playlists_config:
                if p.get('name') == playlist_name:
                    return p.get('category') or p.get('name')
            
            logger.warning(f"Playlist '{playlist_name}' not found in config for video: {video_filename}")
            return None
        except Exception as e:
            logger.error(f"Error getting category for video {video_filename}: {e}")
            return None

    async def update_category_for_video_async(self, video_filename: str, stream_manager) -> bool:
        """
        Asynchronously update stream category based on video filename.
        
        Args:
            video_filename: Filename of the currently playing video
            stream_manager: StreamManager instance for updates
            
        Returns:
            True if successful or no update needed, False if error
        """
        if not video_filename:
            return True
        
        try:
            category = self.get_category_for_video(video_filename)
            if not category:
                return True
            
            # Throttle category updates to prevent spam (only allow one per 3 seconds)
            current_time = time.time()
            if current_time - self._last_category_update_time >= 3:
                # Get current session title to pass with category update
                # Some platforms (like Kick) require title even for category-only updates
                session = self.db.get_current_session()
                stream_title = session.get('stream_title', '') if session else ''
                
                # Use update_stream_info with current title to ensure compatibility
                # This way we update category without changing the title
                await stream_manager.update_stream_info(stream_title, category)
                self._last_category_update_time = current_time
                logger.info(f"Updated category to '{category}' (from video: {video_filename})")
                return True
            else:
                logger.debug(f"Skipping category update for '{category}' - throttled (from video: {video_filename})")
                return True
        except Exception as e:
            logger.error(f"Failed to update category for video {video_filename}: {e}")
            return False

    def get_initial_rotation_category(self, skip_detector, playlist_manager) -> Optional[str]:
        """
        Get the category for the first video in rotation, with fallback to first playlist.
        
        Used during rotation startup to set the correct category for the video about to play.
        
        Args:
            skip_detector: PlaybackSkipDetector instance to get ordered video files
            playlist_manager: PlaylistManager instance to get first playlist as fallback
            
        Returns:
            Category name, or None if unable to determine
        """
        category = None
        
        try:
            # Get first video from the folder
            if skip_detector:
                video_files = skip_detector.get_video_files_in_order()
                if video_files:
                    first_video_filename = video_files[0]
                    # Get category for this specific video
                    category = self.get_category_for_video(first_video_filename)
                    if category:
                        logger.info(f"Got initial rotation category from first video: {first_video_filename} -> {category}")
                        return category
        except Exception as e:
            logger.warning(f"Failed to get category from first video: {e}")
        
        # Fallback: get category from first selected playlist in current session
        try:
            session = self.db.get_current_session()
            if session:
                playlists_selected = session.get('playlists_selected', '')
                if playlists_selected:
                    import json
                    playlist_ids = json.loads(playlists_selected)
                    playlists = playlist_manager.get_playlists_by_ids(playlist_ids)
                    if playlists:
                        category = playlists[0].get('category') or playlists[0].get('name')
                        logger.info(f"Using fallback category from first selected playlist: {category}")
                        return category
        except Exception as e:
            logger.warning(f"Failed to get fallback category from playlist: {e}")
        
        return None

    def truncate_stream_title(self, title: str) -> str:
        """
        Truncate stream title to fit character limit while preserving template.
        
        If the full title exceeds MAX_TITLE_LENGTH, removes playlists from the end
        until it fits. Always keeps the template portion (before first playlist name).
        
        Args:
            title: Full stream title with template and playlist names
        
        Returns:
            Truncated title that fits within MAX_TITLE_LENGTH
        """
        if len(title) <= self.MAX_TITLE_LENGTH:
            return title
        
        # Title is too long, need to truncate by removing playlists from the end
        # Format is typically: "TEMPLATE | PLAYLIST1 | PLAYLIST2 | ..."
        
        # Split on " | " to separate template and playlists
        parts = title.split(' | ')
        if len(parts) < 2:
            # Can't parse, just truncate to limit
            logger.warning(f"Could not parse title for truncation: {title[:50]}...")
            return title[:self.MAX_TITLE_LENGTH]
        
        template = parts[0]  # Keep the template part
        playlists = parts[1:]  # These are the playlist names
        
        # Start with just the template
        result = template
        
        # Add playlists back one by one until it exceeds the limit
        for playlist in playlists:
            candidate = f"{result} | {playlist}"
            if len(candidate) <= self.MAX_TITLE_LENGTH:
                result = candidate
            else:
                # This playlist would make it too long, stop
                break
        
        # Ensure we always end with the separator for consistency
        if not result.endswith(' | ') and len(result) + 3 <= self.MAX_TITLE_LENGTH:
            result += ' | '
        
        logger.info(f"Truncated title from {len(title)} to {len(result)} chars: {result}")
        return result
    
    def prepare_for_switch(self, scene_content_switch: str, vlc_source_name: str) -> bool:
        """
        Prepare for content switch (switch scene, stop VLC).
        
        Args:
            scene_content_switch: Name of content-switch scene
            vlc_source_name: Name of VLC source
            
        Returns:
            True if successful
        """
        self.obs_controller.switch_scene(scene_content_switch)
        self.obs_controller.stop_vlc_source(vlc_source_name)
        time.sleep(3)  # Wait for file locks to release
        return True

    def execute_switch(self, current_folder: str, next_folder: str,
                      is_override_resumption: bool = False,
                      is_override_switch: bool = False,
                      backup_folder: Optional[str] = None) -> bool:
        """
        Execute the actual folder content switch.
        
        Args:
            current_folder: Current video folder
            next_folder: Next/pending folder with new content
            is_override_resumption: If resuming override (add without wiping)
            is_override_switch: If switching for override (backup then wipe+move)
            backup_folder: Where to backup current content (for overrides)
            
        Returns:
            True if successful
        """
        if is_override_resumption:
            # Add override content without wiping
            success = self.playlist_manager.add_override_content(current_folder, next_folder)
        elif is_override_switch and backup_folder:
            # Backup first, then normal switch
            backup_success = self.playlist_manager.backup_current_content(current_folder, backup_folder)
            if not backup_success:
                logger.error("Failed to backup current content for override")
                self.notification_service.notify_rotation_error("Failed to backup content for override")
                return False
            
            # Mark backup success
            suspended_session = self.db.get_suspended_session()
            if suspended_session:
                suspension_data_str = suspended_session.get('suspension_data', '{}')
                try:
                    suspension_data = json.loads(suspension_data_str)
                    suspension_data['backup_success'] = True
                    self.db.update_session_column(
                        suspended_session['id'],
                        'suspension_data',
                        json.dumps(suspension_data)
                    )
                except Exception as e:
                    logger.error(f"Failed to mark backup success: {e}")
            
            # Now do normal switch
            success = self.playlist_manager.switch_content_folders(current_folder, next_folder)
        else:
            # Normal rotation: wipe and switch
            success = self.playlist_manager.switch_content_folders(current_folder, next_folder)
        
        if not success:
            logger.error("Failed to switch content folders")
            self.notification_service.notify_rotation_error("Failed to switch video folders")
            return False
        
        return True

    def finalize_switch(self, current_folder: str, vlc_source_name: str,
                       scene_live: str, scene_offline: str,
                       last_stream_status: Optional[str]) -> bool:
        """
        Finalize content switch (update VLC, switch scene).
        
        Args:
            current_folder: Current video folder
            vlc_source_name: Name of VLC source
            scene_live: Name of live scene
            scene_offline: Name of offline scene
            last_stream_status: Current stream status ("live" or "offline")
            
        Returns:
            True if successful
        """
        # Update VLC source
        success, _ = self.obs_controller.update_vlc_source(vlc_source_name, current_folder)
        if not success:
            logger.error("Failed to update VLC source with new videos")
            return False
        
        # Switch back to appropriate scene
        if last_stream_status == "live":
            self.obs_controller.switch_scene(scene_live)
        else:
            self.obs_controller.switch_scene(scene_offline)
        
        return True

    def update_stream_metadata(self, session_id: Optional[int], stream_manager) -> bool:
        """
        Update stream title and category based on session.
        
        Args:
            session_id: Current session ID
            stream_manager: StreamManager instance for updates
            
        Returns:
            True if successful (or if no session)
        """
        if not session_id:
            return True
        
        session = self.db.get_current_session()
        if not session:
            return True
        
        stream_title = session.get('stream_title', 'Unknown')
        
        # Truncate title to fit platform limits (140 chars for Kick, probably same for Twitch)
        stream_title = self.truncate_stream_title(stream_title)
        
        # Get category from selected playlists
        category = None
        playlists_selected = session.get('playlists_selected', '')
        if playlists_selected:
            try:
                playlist_ids = json.loads(playlists_selected)
                playlists = self.playlist_manager.get_playlists_by_ids(playlist_ids)
                if playlists and len(playlists) > 0:
                    category = playlists[0].get('category') or playlists[0].get('name')
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Could not parse playlists_selected: {e}")
        
        # Update via stream manager
        try:
            stream_manager.update_both(stream_title, category)
            logger.info(f"Updated stream: title='{stream_title}', category='{category}'")
            return True
        except Exception as e:
            logger.error(f"Failed to update stream info: {e}")
            return False

    def mark_playlists_as_played(self, session_id: Optional[int]) -> bool:
        """
        Mark selected playlists as played in database.
        
        Args:
            session_id: Session ID
            
        Returns:
            True if successful
        """
        if not session_id:
            return True
        
        session = self.db.get_current_session()
        if not session:
            return True
        
        playlists_selected = session.get('playlists_selected', '')
        if not playlists_selected:
            return True
        
        try:
            playlist_ids = json.loads(playlists_selected)
            for playlist_id in playlist_ids:
                self.db.update_playlist_played(playlist_id)
            logger.info(f"Marking {len(playlist_ids)} playlists as played: {playlist_ids}")
            return True
        except Exception as e:
            logger.error(f"Failed to mark playlists as played: {e}")
            return False

    def update_category_by_video(self, video_filename: str, stream_manager) -> bool:
        """
        Update stream category based on the currently playing video's source playlist.
        
        This is called when a video transition is detected to update the stream category
        to match the currently playing video's source playlist.
        
        Args:
            video_filename: Filename of the currently playing video
            stream_manager: StreamManager instance for updates
            
        Returns:
            True if successful or no update needed, False if error
        """
        if not video_filename:
            return True
        
        try:
            # Look up the video in database to find its source playlist
            video = self.db.get_video_by_filename(video_filename)
            if not video:
                logger.debug(f"Video not found in database: {video_filename} (may be already deleted)")
                return True
            
            playlist_name = video.get('playlist_name')
            if not playlist_name:
                logger.debug(f"No playlist_name for video: {video_filename}")
                return True
            
            # Get the category for this playlist from playlists config
            playlists_config = self.config.get_playlists()
            target_playlist = None
            for p in playlists_config:
                if p.get('name') == playlist_name:
                    target_playlist = p
                    break
            
            if not target_playlist:
                logger.warning(f"Playlist '{playlist_name}' not found in config for video: {video_filename}")
                return True
            
            category = target_playlist.get('category') or target_playlist.get('name')
            
            # Update via stream manager (async, so schedule it)
            try:
                if category:  # Only create task if category is valid
                    # Throttle category updates to prevent spam (only allow one per 3 seconds)
                    current_time = time.time()
                    if current_time - self._last_category_update_time >= 3:
                        asyncio.create_task(stream_manager.update_category(category))  # Use update_category instead of update_stream_info with None title
                        self._last_category_update_time = current_time
                        logger.info(f"Updated category to '{category}' (from video: {video_filename})")
                    else:
                        logger.debug(f"Skipping category update for '{category}' - throttled (from video: {video_filename})")
                    return True
                else:
                    logger.warning(f"No valid category for video: {video_filename}")
                    return True
            except Exception as e:
                logger.error(f"Failed to update category: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating category by video: {e}")
            return False
