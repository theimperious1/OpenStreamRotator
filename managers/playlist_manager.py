import os
import logging
import shutil
from typing import List, Dict, Optional
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from core.video_registration_queue import VideoRegistrationQueue
from utils.playlist_selector import PlaylistSelector
from utils.video_downloader import VideoDownloader
import re

logger = logging.getLogger(__name__)


class PlaylistManager:
    def __init__(self, db: DatabaseManager, config: ConfigManager, 
                 registration_queue: Optional[VideoRegistrationQueue] = None):
        self.db = db
        self.config = config
        self.registration_queue = registration_queue
        self.selector = PlaylistSelector(db, config)
        self.downloader = VideoDownloader(db, config, registration_queue)

    def get_playlists_by_ids(self, playlist_ids: List[int]) -> List[Dict]:
        """Get full playlist data from config by their database IDs.
        
        Note: This retrieves the config metadata (including category) for playlists
        by looking up their names from the database.
        """
        config_playlists = self.config.get_playlists()
        
        # Get playlist names from database by their IDs
        playlist_names = []
        for pid in playlist_ids:
            playlist = self.db.get_playlist(pid)
            if playlist:
                playlist_names.append(playlist.get('name'))
        
        # Find matching playlists in config
        result = []
        for name in playlist_names:
            for p in config_playlists:
                if p.get('name') == name:
                    result.append(p)
                    break
        
        return result
    
    def get_playlists_by_names(self, playlist_names: List[str]) -> List[Dict]:
        """Get full playlist data from config by their names.
        
        Used to restore prepared playlists from database by name.
        """
        config_playlists = self.config.get_playlists()
        
        # Find matching playlists in config by name
        result = []
        for name in playlist_names:
            for p in config_playlists:
                if p.get('name') == name:
                    result.append(p)
                    break
        
        return result
    
    def select_playlists_for_rotation(self, manual_selection: Optional[List[str]] = None) -> List[Dict]:
        """
        Select playlists for the next rotation.
        Uses manual selection if provided, otherwise automatic selection.
        Only selects playlists that are defined in the config file.
        """
        return self.selector.select_for_rotation(manual_selection)

    def download_playlists(self, playlists: List[Dict], output_folder: str, verbose: bool = False) -> Dict:
        """
        Download selected playlists using yt-dlp.
        Returns dict with 'success' bool and 'total_duration_seconds' from all videos.
        
        Args:
            playlists: List of playlist dictionaries to download
            output_folder: Output folder for videos
            verbose: If True, enable verbose yt-dlp logging for debugging
        
        Returns:
            Dict with 'success' and 'total_duration_seconds' keys
        """
        return self.downloader.download_playlists(playlists, output_folder, verbose=verbose)

    def extract_playlists_from_folder(self, folder: str) -> List[str]:
        """Extract unique playlist names from files in folder."""
        playlists = set()
        video_extensions = ('.mp4', '.mkv', '.avi', '.webm', '.flv', '.mov')

        if not os.path.exists(folder):
            return []

        for filename in os.listdir(folder):
            if filename.lower().endswith(video_extensions):
                # Extract playlist name (everything before _NUMBER_)
                match = re.match(r'^(.+?)_\d+_', filename)
                if match:
                    playlists.add(match.group(1))

        return sorted(list(playlists))

    def generate_stream_title(self, playlists: List[str]) -> str:
        """Generate stream title based on current playlists."""
        settings = self.config.get_settings()
        template = settings.get(
            'stream_title_template',
            '24/7 @example1 / @example2 | {GAMES} | !playlist !streamtime !new'
        )

        games_str = ' | '.join(p.upper() for p in playlists) if playlists else 'VARIETY'

        title = template.replace('{GAMES}', games_str)
        return title

    def switch_content_folders(self, current_folder: str, next_folder: str) -> bool:
        """
        Switch content: delete current folder contents, move next folder contents to current.
        """
        try:
            # Delete current folder contents
            if os.path.exists(current_folder):
                for filename in os.listdir(current_folder):
                    file_path = os.path.join(current_folder, filename)
                    try:
                        if os.path.isfile(file_path):
                            os.unlink(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        logger.error(f"Error deleting {file_path}: {e}")

            # Move next folder contents to current folder
            if os.path.exists(next_folder):
                for filename in os.listdir(next_folder):
                    src = os.path.join(next_folder, filename)
                    dst = os.path.join(current_folder, filename)
                    try:
                        shutil.move(src, dst)
                    except Exception as e:
                        logger.error(f"Error moving {src} to {dst}: {e}")

            logger.info("Content folders switched successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to switch content folders: {e}")
            return False

    def backup_current_content(self, current_folder: str, backup_folder: str) -> bool:
        """
        Backup current folder contents to backup folder.
        Used before override to preserve original content.
        """
        try:
            # Normalize paths to avoid Windows path issues
            current_folder = os.path.normpath(current_folder)
            backup_folder = os.path.normpath(backup_folder)
            
            # Create backup folder
            os.makedirs(backup_folder, exist_ok=True)
            
            # Move current folder contents to backup
            if os.path.exists(current_folder):
                for filename in os.listdir(current_folder):
                    src = os.path.join(current_folder, filename)
                    dst = os.path.join(backup_folder, filename)
                    try:
                        if os.path.isfile(src) or os.path.isdir(src):
                            shutil.move(src, dst)
                            logger.info(f"Backed up: {filename}")
                    except Exception as e:
                        logger.error(f"Error backing up {src} to {dst}: {e}")
            
            logger.info(f"Backup complete: {current_folder} → {backup_folder}")
            return True

        except Exception as e:
            logger.error(f"Failed to backup content: {e}")
            return False

    def restore_content_after_override(self, current_folder: str, backup_folder: str) -> bool:
        """
        Restore original content from backup folder after override completes.
        Cleans up override content and restores original.
        """
        try:
            import time
            
            # Normalize paths to avoid Windows path issues
            current_folder = os.path.normpath(current_folder)
            backup_folder = os.path.normpath(backup_folder)
            
            # Delete override content from current folder
            # Use retry logic since VLC might still hold file locks briefly
            if os.path.exists(current_folder):
                for filename in os.listdir(current_folder):
                    file_path = os.path.join(current_folder, filename)
                    deleted = False
                    # Try 5 times with longer delays (1.5s) to handle OS file locking
                    for attempt in range(5):
                        try:
                            if os.path.isfile(file_path):
                                os.unlink(file_path)
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                            deleted = True
                            break
                        except (PermissionError, OSError) as e:
                            if attempt < 4:  # Not the last attempt
                                time.sleep(1.5)  # Longer delay before retry
                            else:
                                logger.error(f"CRITICAL: Could not delete {filename} after 5 attempts (6s total) - file may be locked: {e}")
                    
                    if deleted:
                        logger.info(f"Deleted override: {filename}")
                    else:
                        logger.error(f"CRITICAL: Failed to delete override file {filename} - restoration may be incomplete!")
            
            # Move backup folder contents back to current
            if os.path.exists(backup_folder):
                for filename in os.listdir(backup_folder):
                    src = os.path.join(backup_folder, filename)
                    dst = os.path.join(current_folder, filename)
                    try:
                        shutil.move(src, dst)
                        logger.info(f"Restored: {filename}")
                    except Exception as e:
                        logger.error(f"Error restoring {src} to {dst}: {e}")
            
            # Clean up backup folder
            try:
                if os.path.exists(backup_folder):
                    os.rmdir(backup_folder)
                    logger.info(f"Cleaned up backup folder: {backup_folder}")
            except Exception as e:
                logger.warning(f"Could not remove backup folder {backup_folder}: {e}")
            
            logger.info(f"Restore complete: {backup_folder} → {current_folder}")
            return True

        except Exception as e:
            logger.error(f"Failed to restore content: {e}")
            return False

    def add_override_content(self, current_folder: str, next_folder: str) -> bool:
        """
        Add override content to current folder WITHOUT wiping existing content.
        This is used when temporarily overriding but preserving original content.
        """
        try:
            # Ensure current folder exists
            os.makedirs(current_folder, exist_ok=True)
            
            # Move next folder contents to current folder (but don't delete current)
            if os.path.exists(next_folder):
                for filename in os.listdir(next_folder):
                    src = os.path.join(next_folder, filename)
                    dst = os.path.join(current_folder, filename)
                    try:
                        # Skip if file already exists with same name
                        if not os.path.exists(dst):
                            shutil.move(src, dst)
                        else:
                            logger.warning(f"File already exists, skipping: {filename}")
                    except Exception as e:
                        logger.error(f"Error moving {src} to {dst}: {e}")

            logger.info("Override content added to current folder")
            return True

        except Exception as e:
            logger.error(f"Failed to add override content: {e}")
            return False

    def validate_downloads(self, folder: str) -> bool:
        """Validate that downloads completed successfully."""
        video_extensions = ('.mp4', '.mkv', '.avi', '.webm', '.flv', '.mov')

        if not os.path.exists(folder):
            logger.error(f"Folder does not exist: {folder}")
            return False

        video_files = [f for f in os.listdir(folder)
                       if f.lower().endswith(video_extensions)]

        if len(video_files) == 0:
            logger.error("No video files found in download folder")
            return False

        # Check that all files have non-zero size
        for filename in video_files:
            file_path = os.path.join(folder, filename)
            if os.path.getsize(file_path) == 0:
                logger.error(f"Zero-size file detected: {filename}")
                return False

        logger.info(f"Validated {len(video_files)} video files")
        return True