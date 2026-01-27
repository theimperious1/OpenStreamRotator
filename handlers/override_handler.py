import os
import json
import shutil
import logging
from datetime import datetime
from typing import Dict, Optional, List
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from managers.playlist_manager import PlaylistManager
from services.notification_service import NotificationService

logger = logging.getLogger(__name__)


class OverrideHandler:
    """Handles manual override requests and execution."""

    def __init__(self, db: DatabaseManager, config: ConfigManager,
                 playlist_manager: PlaylistManager, notification_service: NotificationService,
                 playback_tracker):
        """
        Initialize override handler.
        
        Args:
            db: DatabaseManager instance
            config: ConfigManager instance
            playlist_manager: PlaylistManager instance
            notification_service: NotificationService instance
            playback_tracker: PlaybackTracker instance
        """
        self.db = db
        self.config = config
        self.playlist_manager = playlist_manager
        self.notification_service = notification_service
        self.playback_tracker = playback_tracker

    def check_override_triggered(self) -> bool:
        """Check if override config has changed."""
        return self.config.has_override_changed()

    def get_active_override(self) -> Optional[Dict]:
        """Get active override if one exists."""
        return self.config.get_active_override()

    def validate_override(self, override: Dict) -> bool:
        """
        Validate override configuration.
        
        Returns:
            True if override is valid, False otherwise
        """
        if not override:
            logger.warning("Override is empty")
            return False
        
        selected = override.get('selected_playlists', [])
        if not selected:
            logger.warning("No playlists selected in override")
            return False
        
        return True

    def sync_config_playlists(self):
        """Sync config playlists to database before override."""
        config_playlists = self.config.get_playlists()
        self.db.sync_playlists_from_config(config_playlists)

    def backup_prepared_rotation(self, next_folder: str) -> str:
        """
        Backup the prepared next rotation (if it exists).
        
        Args:
            next_folder: Path to /pending folder
            
        Returns:
            Path to backup folder
        """
        settings = self.config.get_settings()
        base_path = os.path.dirname(settings.get('video_folder', 'C:/stream_videos/'))
        pending_backup_folder = os.path.normpath(os.path.join(base_path, 'temp_pending_backup'))
        
        if not os.path.exists(next_folder) or not os.listdir(next_folder):
            logger.info("No prepared rotation to backup")
            return pending_backup_folder
        
        try:
            logger.info(f"Saving prepared next rotation from {next_folder} to {pending_backup_folder}")
            os.makedirs(pending_backup_folder, exist_ok=True)
            
            for filename in os.listdir(next_folder):
                src = os.path.join(next_folder, filename)
                dst = os.path.join(pending_backup_folder, filename)
                try:
                    shutil.move(src, dst)
                    logger.info(f"Saved prepared: {filename}")
                except Exception as e:
                    logger.error(f"Error saving pending content {src}: {e}")
            
            logger.info(f"Pending backup complete: {next_folder} → {pending_backup_folder}")
            return pending_backup_folder
            
        except Exception as e:
            logger.error(f"Error backing up prepared rotation: {e}")
            return pending_backup_folder

    def suspend_current_session(self, current_session_id: Optional[int], 
                               next_prepared_playlists: Optional[List],
                               download_in_progress: bool,
                               override: Dict) -> Dict:
        """
        Suspend current session for override.
        
        Args:
            current_session_id: ID of current session
            next_prepared_playlists: Pre-selected playlists (if any)
            download_in_progress: If background download is in progress
            override: Override configuration
            
        Returns:
            Dictionary with suspension data
        """
        if not current_session_id:
            logger.info("No active session to suspend")
            return {}
        
        total_seconds = self.playback_tracker.get_total_seconds()
        self.db.update_session_playback(current_session_id, total_seconds)
        
        # Get backup folder path
        settings = self.config.get_settings()
        base_path = os.path.dirname(settings.get('video_folder', 'C:/stream_videos/'))
        backup_folder = os.path.normpath(os.path.join(base_path, 'temp_backup_override'))
        pending_backup_folder = os.path.normpath(os.path.join(base_path, 'temp_pending_backup'))
        
        # Store prepared playlists if available and no download in progress
        prepared_playlist_names = []
        if next_prepared_playlists and not download_in_progress:
            prepared_playlist_names = [p['name'] for p in next_prepared_playlists]
            logger.info(f"Saving prepared playlists for restore: {prepared_playlist_names}")
        elif download_in_progress:
            logger.warning("Override triggered during background download - will force fresh download after override")
        else:
            # Check if this session already has suspension data with prepared playlists (nested override case)
            session = self.db.get_session_by_id(current_session_id)
            if session and session.get('suspension_data'):
                try:
                    existing_suspension = json.loads(session['suspension_data']) if isinstance(session['suspension_data'], str) else session['suspension_data']
                    existing_prepared = existing_suspension.get('prepared_playlist_names', [])
                    if existing_prepared:
                        prepared_playlist_names = existing_prepared
                        logger.info(f"Preserving prepared playlists from previous suspension: {prepared_playlist_names}")
                except (json.JSONDecodeError, AttributeError):
                    pass
        
        suspension_data = {
            "suspended_by_override": True,
            "override_playlists": override.get('selected_playlists', []),
            "playback_seconds_before_override": total_seconds,
            "backup_folder": backup_folder,
            "pending_backup_folder": pending_backup_folder,
            "prepared_playlist_names": prepared_playlist_names
        }
        
        self.db.suspend_session(current_session_id, suspension_data)
        logger.info(f"Suspended session {current_session_id} for override")
        
        return suspension_data

    def start_override_rotation(self, selected_playlists: List[str], next_folder: str) -> bool:
        """
        Start rotation with manually selected playlists.
        
        Note: Downloads are handled by the controller's start_rotation_session().
        This method just validates and prepares the override.
        
        Args:
            selected_playlists: List of playlist names to play
            next_folder: Output folder for downloads
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting override rotation with: {selected_playlists}")
        
        # Get playlist objects from names to validate they exist
        all_playlists = self.db.get_enabled_playlists()
        selected_playlist_objs = [p for p in all_playlists if p['name'] in selected_playlists]
        
        if not selected_playlist_objs:
            logger.error("Selected playlists not found in database")
            return False
        
        # Send notification about override start
        self.notification_service.notify_rotation_started([p['name'] for p in selected_playlist_objs])
        
        logger.info(f"Override validation complete: {len(selected_playlist_objs)} playlists ready")
        return True

    def clear_override(self):
        """Clear override after it's been processed."""
        self.config.clear_override()
        logger.info("Manual override cleared")
