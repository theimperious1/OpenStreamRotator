import json
import logging
import time
from typing import Optional
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from managers.playlist_manager import PlaylistManager
from controllers.obs_controller import OBSController
from services.notification_service import NotificationService

logger = logging.getLogger(__name__)


class ContentSwitchHandler:
    """Handles content switching operations (normal rotations and overrides)."""

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
        if not self.obs_controller.update_vlc_source(vlc_source_name, current_folder):
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
