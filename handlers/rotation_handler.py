import os
import shutil
import time
import logging
from datetime import datetime
from typing import Optional, Tuple
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from managers.playlist_manager import PlaylistManager
from services.playback_skip_detector import PlaybackSkipDetector
from services.notification_service import NotificationService

logger = logging.getLogger(__name__)


class RotationHandler:
    """Handles rotation checks and transitions."""

    def __init__(self, db: DatabaseManager, config: ConfigManager, 
                 playlist_manager: PlaylistManager, playback_skip_detector: Optional[PlaybackSkipDetector],
                 notification_service: NotificationService, playback_tracker):
        """
        Initialize rotation handler.
        
        Args:
            db: DatabaseManager instance
            config: ConfigManager instance
            playlist_manager: PlaylistManager instance
            playback_skip_detector: PlaybackSkipDetector instance (can be None)
            notification_service: NotificationService instance
            playback_tracker: PlaybackTracker instance for getting total playback seconds
        """
        self.db = db
        self.config = config
        self.playlist_manager = playlist_manager
        self.playback_skip_detector = playback_skip_detector
        self.notification_service = notification_service
        self.playback_tracker = playback_tracker
        
        self._rotation_duration_reached_logged = False

    def set_playback_skip_detector(self, detector: Optional[PlaybackSkipDetector]):
        """Update the skip detector reference (called after detector is initialized)."""
        self.playback_skip_detector = detector
        logger.debug(f"Updated skip detector reference in rotation handler: {detector is not None}")

    def check_skip_detection(self, current_session_id: Optional[int]) -> Tuple[bool, Optional[dict]]:
        """
        Check for playback skip and recalculate times if needed.
        
        Returns:
            Tuple of (skip_detected, skip_info)
        """
        if not self.playback_skip_detector:
            logger.warning("Skip detector not initialized - cannot check for skips")
            return False, None
        
        skip_detected, skip_info = self.playback_skip_detector.check_for_skip(current_session_id)
        if skip_detected and skip_info:
            logger.info(f"Skip detection result: SKIP DETECTED - {skip_info['time_skipped_seconds']:.1f}s skipped")
        return skip_detected, skip_info

    def trigger_background_download(self, next_prepared_playlists, download_in_progress):
        """
        Check if background download should be triggered.
        
        Note: This returns playlists to download. The actual executor submission
        happens in the main loop to keep this handler decoupled from asyncio.
        
        Returns:
            List of playlists to download, or None if download shouldn't trigger
        """
        if download_in_progress or next_prepared_playlists is not None:
            return None
        
        # Check if we have prepared content backed up waiting to be restored
        # (happens when override completes and restores prepared rotation)
        settings = self.config.get_settings()
        base_path = os.path.dirname(settings.get('video_folder', 'C:/stream_videos/'))
        pending_backup_folder = os.path.normpath(os.path.join(base_path, 'temp_pending_backup'))
        
        if os.path.exists(pending_backup_folder) and os.listdir(pending_backup_folder):
            logger.debug(f"Pending backup folder has content, skipping background download")
            return None
        
        # Select playlists in main thread (can't be done in executor thread due to SQLite)
        playlists = self.playlist_manager.select_playlists_for_rotation()
        return playlists if playlists else None

    def check_rotation_duration(self, session: dict) -> bool:
        """
        Check if rotation duration has been reached.
        
        Returns:
            True if rotation duration reached, False otherwise
        """
        if not session.get('estimated_finish_time'):
            return False
        
        finish_time = datetime.fromisoformat(session['estimated_finish_time'])
        return datetime.now() >= finish_time

    def get_rotation_completion_info(self, session: dict) -> Tuple[int, bool]:
        """
        Get info needed for rotation completion.
        
        Returns:
            Tuple of (total_seconds, has_suspended_session)
        """
        total_seconds = self.playback_tracker.get_total_seconds()
        suspended_session = self.db.get_suspended_session()
        has_suspended = suspended_session is not None
        
        return total_seconds, has_suspended

    def log_rotation_completion(self, total_seconds: int):
        """Log rotation completion (once per session)."""
        if not self._rotation_duration_reached_logged:
            logger.info(f"Rotation duration reached: {total_seconds}s")
            self._rotation_duration_reached_logged = True

    def reset_rotation_log_flag(self):
        """Reset the rotation duration log flag for new session."""
        self._rotation_duration_reached_logged = False

    def restore_after_override(self, current_folder: str, backup_folder: str, 
                              pending_backup_folder: str, prepared_playlist_names: list,
                              obs_controller, vlc_source_name: str, scene_content_switch: str) -> bool:
        """
        Restore original content after override completes.
        
        Args:
            current_folder: Current video folder
            backup_folder: Where original content is backed up
            pending_backup_folder: Where prepared next rotation was saved
            prepared_playlist_names: Names of playlists to restore to next_prepared_playlists
            obs_controller: OBS controller for scene/VLC operations
            vlc_source_name: Name of VLC source
            scene_content_switch: Name of content-switch scene
            
        Returns:
            True if successful, False otherwise
        """
        if not backup_folder or not os.path.exists(backup_folder):
            logger.warning("No backup folder available, skipping restore")
            return False
        
        try:
            logger.info(f"Restoring original content from {backup_folder}")
            
            # Stop VLC BEFORE attempting file operations
            if obs_controller:
                obs_controller.switch_scene(scene_content_switch)
                obs_controller.stop_vlc_source(vlc_source_name)
                time.sleep(3)  # Wait for OS to release file handles
            
            # Delete override content
            if os.path.exists(current_folder):
                for filename in os.listdir(current_folder):
                    file_path = os.path.join(current_folder, filename)
                    deleted = False
                    
                    # Try 5 times with 1.5s delays for file lock handling
                    for attempt in range(5):
                        try:
                            if os.path.isfile(file_path):
                                os.unlink(file_path)
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                            deleted = True
                            break
                        except (PermissionError, OSError) as e:
                            if attempt < 4:
                                time.sleep(1.5)
                            else:
                                logger.error(f"CRITICAL: Could not delete {filename} after 5 attempts: {e}")
                    
                    if deleted:
                        logger.info(f"Deleted override: {filename}")
            
            # Restore original content
            if os.path.exists(backup_folder):
                for filename in os.listdir(backup_folder):
                    src = os.path.join(backup_folder, filename)
                    dst = os.path.join(current_folder, filename)
                    try:
                        shutil.move(src, dst)
                        logger.info(f"Restored: {filename}")
                    except Exception as e:
                        logger.error(f"Error restoring {filename}: {e}")
            
            # Clean up backup folder
            try:
                if os.path.exists(backup_folder):
                    os.rmdir(backup_folder)
                    logger.info(f"Cleaned up backup folder: {backup_folder}")
            except Exception as e:
                logger.warning(f"Could not remove backup folder: {e}")
            
            logger.info(f"Restore complete: {backup_folder} → {current_folder}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore content: {e}")
            return False

    def restore_prepared_rotation(self, pending_backup_folder: str, next_folder: str,
                                  prepared_playlist_names: list) -> Tuple[bool, list]:
        """
        Restore prepared next rotation from backup.
        
        Returns:
            Tuple of (success, restored_playlist_objects)
        """
        if not pending_backup_folder or not os.path.exists(pending_backup_folder):
            logger.info("No pending backup, skipping prepared rotation restore")
            return True, []
        
        try:
            logger.info(f"Restoring prepared next rotation from {pending_backup_folder}")
            
            # Clear any remnants from /pending
            os.makedirs(next_folder, exist_ok=True)
            for filename in os.listdir(next_folder):
                try:
                    file_path = os.path.join(next_folder, filename)
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    logger.error(f"Error clearing {filename} from {next_folder}: {e}")
            
            # Restore from backup
            for filename in os.listdir(pending_backup_folder):
                src = os.path.join(pending_backup_folder, filename)
                dst = os.path.join(next_folder, filename)
                try:
                    shutil.move(src, dst)
                    logger.info(f"Restored prepared content: {filename}")
                except Exception as e:
                    logger.error(f"Error restoring {filename}: {e}")
            
            # Clean up backup folder
            try:
                shutil.rmtree(pending_backup_folder)
                logger.info(f"Cleaned up pending backup folder: {pending_backup_folder}")
            except Exception as e:
                logger.error(f"Error cleaning pending backup folder: {e}")
            
            # Restore prepared playlists list
            restored_playlists = []
            if prepared_playlist_names:
                all_playlists = self.db.get_enabled_playlists()
                for playlist in all_playlists:
                    if playlist['name'] in prepared_playlist_names:
                        restored_playlists.append(playlist)
            
            logger.info(f"Prepared rotation restore complete. Restored playlists: {[p['name'] for p in restored_playlists]}")
            return True, restored_playlists
            
        except Exception as e:
            logger.error(f"Failed to restore prepared rotation: {e}")
            return False, []
