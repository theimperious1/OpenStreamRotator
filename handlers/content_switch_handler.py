import json
import logging
import time
from typing import Optional
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from managers.playlist_manager import PlaylistManager
from controllers.obs_controller import OBSController
from services.notification_service import NotificationService
from utils.video_utils import strip_ordering_prefix, resolve_category_for_video, get_video_files_sorted

logger = logging.getLogger(__name__)


class ContentSwitchHandler:
    """Handles content switching operations (normal rotations)."""

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
        return resolve_category_for_video(video_filename, self.db, self.config)

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
                # Only update category - title is managed separately at rotation/temp playback boundaries
                await stream_manager.update_category(category)
                
                self._last_category_update_time = current_time
                logger.info(f"Updated category to '{category}' (from video: {video_filename})")
                return True
            else:
                logger.debug(f"Skipping category update for '{category}' - throttled (from video: {video_filename})")
                return True
        except Exception as e:
            logger.error(f"Failed to update category for video {video_filename}: {e}")
            return False

    def get_initial_rotation_category(self, video_folder: str, playlist_manager) -> Optional[str]:
        """
        Get the category for the first video in rotation, with fallback to first playlist.
        
        Used during rotation startup to set the correct category for the video about to play.
        
        Args:
            video_folder: Path to the video folder to scan
            playlist_manager: PlaylistManager instance to get first playlist as fallback
            
        Returns:
            Category name, or None if unable to determine
        """
        category = None
        
        try:
            # Get first video from the folder (sorted alphabetically, matching VLC order)
            video_files = get_video_files_sorted(video_folder)
            if video_files:
                first_video = video_files[0]
                original_name = strip_ordering_prefix(first_video)
                category = self.get_category_for_video(original_name)
                if category:
                    logger.info(f"Got initial rotation category from first video: {first_video} -> {category}")
                    return category
        except Exception as e:
            logger.warning(f"Failed to get category from first video: {e}")
        
        # Fallback: get category from first selected playlist in current session
        try:
            session = self.db.get_current_session()
            if session:
                playlists_selected = session.get('playlists_selected', '')
                if playlists_selected:
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
    
    def prepare_for_switch(self, scene_rotation_screen: str, vlc_source_name: str) -> bool:
        """
        Prepare for content switch (switch scene, stop VLC).
        
        Args:
            scene_rotation_screen: Name of Rotation screen scene
            vlc_source_name: Name of VLC source
            
        Returns:
            True if successful
        """
        self.obs_controller.switch_scene(scene_rotation_screen)
        self.obs_controller.stop_vlc_source(vlc_source_name)
        time.sleep(3)  # Wait for file locks to release
        return True

    def execute_switch(self, current_folder: str, next_folder: str) -> bool:
        """
        Execute the actual folder content switch.
        
        Args:
            current_folder: Current video folder
            next_folder: Next/pending folder with new content
            
        Returns:
            True if successful
        """
        # Normal rotation: wipe and switch
        success = self.playlist_manager.switch_content_folders(current_folder, next_folder)
        
        if not success:
            logger.error("Failed to switch content folders")
            self.notification_service.notify_rotation_error("Failed to switch video folders")
            return False
        
        return True

    def finalize_switch(self, current_folder: str, vlc_source_name: str,
                       scene_pause: str, scene_stream: str,
                       last_stream_status: Optional[str]) -> tuple[bool, list]:
        """
        Finalize content switch (update VLC, switch scene).
        
        Args:
            current_folder: Current video folder
            vlc_source_name: Name of VLC source
            scene_pause: Name of pause scene (shown when streamer is live)
            scene_stream: Name of stream scene (normal 24/7 playback)
            last_stream_status: Current stream status ("live" or "offline")
            
        Returns:
            Tuple of (success: bool, playlist: list[str])
        """
        # Update VLC source and get the playlist
        success, playlist = self.obs_controller.update_vlc_source(vlc_source_name, current_folder)
        if not success:
            logger.error("Failed to update VLC source with new videos")
            return False, []
        
        # Switch back to appropriate scene
        if last_stream_status == "live":
            self.obs_controller.switch_scene(scene_pause)
        else:
            self.obs_controller.switch_scene(scene_stream)
        
        return True, playlist

    async def update_stream_metadata(self, session_id: Optional[int], stream_manager) -> bool:
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
            await stream_manager.update_both(stream_title, category)
            logger.info(f"Updated stream: title='{stream_title}', category='{category}'")
            return True
        except Exception as e:
            logger.error(f"Failed to update stream info: {e}")
            return False


