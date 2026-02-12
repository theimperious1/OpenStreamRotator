"""Playlist operations — selection, downloading, and folder management.

Facade over PlaylistSelector and VideoDownloader, plus folder
operations like content switching, validation, and cleanup.
"""
import os
import logging
import shutil
from threading import Event
from typing import List, Dict, Optional
from config.constants import VIDEO_EXTENSIONS
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from core.video_registration_queue import VideoRegistrationQueue
from utils.playlist_selector import PlaylistSelector
from utils.video_downloader import VideoDownloader
import re

logger = logging.getLogger(__name__)


class PlaylistManager:
    def __init__(self, db: DatabaseManager, config: ConfigManager, 
                 registration_queue: Optional[VideoRegistrationQueue] = None,
                 shutdown_event: Optional[Event] = None):
        self.db = db
        self.config = config
        self.registration_queue = registration_queue
        self.selector = PlaylistSelector(db, config)
        self.downloader = VideoDownloader(db, config, registration_queue, shutdown_event=shutdown_event)

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

        if not os.path.exists(folder):
            return []

        for filename in os.listdir(folder):
            if filename.lower().endswith(VIDEO_EXTENSIONS):
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
        Excludes archive.txt from move (used by yt-dlp to track downloads) and deletes it after.
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
                    # Skip the temp folder - it's for yt-dlp downloads, not for live playback
                    if filename == 'temp':
                        continue
                    # Skip archive.txt - it's used by yt-dlp to track downloaded videos
                    # and should not be moved to live folder
                    if filename == 'archive.txt':
                        continue
                    src = os.path.join(next_folder, filename)
                    dst = os.path.join(current_folder, filename)
                    try:
                        shutil.move(src, dst)
                    except Exception as e:
                        logger.error(f"Error moving {src} to {dst}: {e}")

            # Delete archive.txt after rotation completes so next rotation starts fresh
            archive_path = os.path.join(next_folder, 'archive.txt')
            if os.path.exists(archive_path):
                try:
                    os.unlink(archive_path)
                    logger.info("Deleted archive.txt after rotation")
                except Exception as e:
                    logger.warning(f"Could not delete archive.txt: {e}")

            logger.info("Content folders switched successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to switch content folders: {e}")
            return False

    def backup_current_content(self, current_folder: str, backup_folder: str) -> bool:
        """
        Backup current folder contents to backup folder.
        Preserves original content before switching.
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

    def validate_downloads(self, folder: str) -> bool:
        """Validate that downloads completed successfully."""

        if not os.path.exists(folder):
            logger.error(f"Folder does not exist: {folder}")
            return False

        video_files = [f for f in os.listdir(folder)
                       if f.lower().endswith(VIDEO_EXTENSIONS)]

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

    def is_folder_empty(self, folder: str) -> bool:
        """Check if a folder is empty or doesn't exist (only counts video files, not metadata).
        
        Args:
            folder: Path to folder to check
            
        Returns:
            True if folder is empty/missing or contains no video files, False otherwise
        """
        try:
            if not os.path.exists(folder):
                return True
            
            # Check if folder has any files (ignore subdirectories, archive.txt, and temp folder)
            # archive.txt is yt-dlp's download tracking file, not actual content
            items = [entry.name for entry in os.scandir(folder) 
                     if entry.is_file() and entry.name != 'archive.txt']
            return len(items) == 0
        except Exception as e:
            logger.warning(f"Error checking folder {folder}: {e}")
            return False  # Conservative: assume not empty if we can't check

    def get_complete_video_files(self, folder: str) -> list:
        """
        Get list of complete video files in a folder.
        
        Note: yt-dlp is configured to separate temp files (fragments, .part, .ytdl)
        into a 'temp' subfolder, so this only needs to check for video extensions.
        """
        if not os.path.exists(folder):
            return []
        complete_files = []
        for filename in os.listdir(folder):
            # Skip subdirectories (including 'temp' folder)
            if os.path.isdir(os.path.join(folder, filename)):
                continue
            # Include files with video extensions
            if filename.lower().endswith(VIDEO_EXTENSIONS):
                complete_files.append(filename)
        return complete_files

    def cleanup_temp_downloads(self, folder: str) -> bool:
        """
        Clean up temporary download files in the temp subfolder.
        
        yt-dlp stores all temporary files (fragments, .part, .ytdl) in folder/temp/.
        This should be called after each successful rotation to clean up old metadata.
        """
        temp_folder = os.path.join(folder, 'temp')
        if not os.path.exists(temp_folder):
            return True  # Nothing to clean
        
        try:
            for filename in os.listdir(temp_folder):
                filepath = os.path.join(temp_folder, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
                elif os.path.isdir(filepath):
                    shutil.rmtree(filepath)
            logger.info(f"Cleaned up temp downloads folder: {temp_folder}")
            return True
        except Exception as e:
            logger.error(f"Error cleaning up temp downloads folder: {e}")
            return False

    def move_files_to_folder(self, source_folder: str, dest_folder: str, filenames: list) -> bool:
        """Move specific files from source to destination folder."""
        try:
            os.makedirs(dest_folder, exist_ok=True)
            for filename in filenames:
                src = os.path.join(source_folder, filename)
                dst = os.path.join(dest_folder, filename)
                if os.path.exists(src):
                    shutil.move(src, dst)
                    logger.info(f"Moved: {filename} → {os.path.basename(dest_folder)}/")
            return True
        except Exception as e:
            logger.error(f"Error moving files: {e}")
            return False

    def copy_files_to_folder(self, source_folder: str, dest_folder: str, filenames: list) -> bool:
        """Copy specific files from source to destination folder (do not move)."""
        try:
            os.makedirs(dest_folder, exist_ok=True)
            for filename in filenames:
                src = os.path.join(source_folder, filename)
                dst = os.path.join(dest_folder, filename)
                if os.path.exists(src):
                    shutil.copy2(src, dst)
                    logger.info(f"Copied: {filename} → {os.path.basename(dest_folder)}/")
            return True
        except Exception as e:
            logger.error(f"Error copying files: {e}")
            return False

    def rename_videos_with_playlist_prefix(self, folder: str, playlist_order: list[str]) -> bool:
        """Rename video files with ordering prefix based on their source playlist.
        
        Files are prefixed with 'XX_' where XX is the playlist's position in the
        rotation order (01, 02, etc.). This ensures alphabetical sorting groups
        videos by playlist and plays them in the correct order.
        
        Args:
            folder: Path to the folder containing video files to rename
            playlist_order: Ordered list of playlist names (e.g., ['CATS', 'MW2'])
        
        Returns:
            True if successful
        """
        if not os.path.exists(folder) or not playlist_order:
            return True
        
        try:
            # Build a lookup: playlist_name -> prefix index
            playlist_prefix = {}
            for i, name in enumerate(playlist_order):
                playlist_prefix[name] = f"{i + 1:02d}"
            
            renamed_count = 0
            for filename in os.listdir(folder):
                if not filename.lower().endswith(VIDEO_EXTENSIONS):
                    continue
                
                # Skip files that already have a prefix
                if re.match(r'^\d{2}_', filename):
                    continue
                
                # Look up this video's playlist in the database
                video = self.db.get_video_by_filename(filename)
                if not video:
                    logger.debug(f"Video not in database, skipping prefix: {filename}")
                    continue
                
                playlist_name = video.get('playlist_name', '')
                prefix = playlist_prefix.get(playlist_name, '99')  # Default to end
                
                new_filename = f"{prefix}_{filename}"
                src = os.path.join(folder, filename)
                dst = os.path.join(folder, new_filename)
                
                os.rename(src, dst)
                renamed_count += 1
            
            if renamed_count > 0:
                logger.info(f"Renamed {renamed_count} videos with playlist prefix in {folder}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rename videos with prefix: {e}")
            return False

    def merge_folders_to_destination(self, source_folders: list, dest_folder: str) -> bool:
        """Merge contents from multiple source folders into destination folder."""
        try:
            os.makedirs(dest_folder, exist_ok=True)
            for source_folder in source_folders:
                if not os.path.exists(source_folder):
                    continue
                for filename in os.listdir(source_folder):
                    src = os.path.join(source_folder, filename)
                    dst = os.path.join(dest_folder, filename)
                    if os.path.isfile(src):
                        shutil.move(src, dst)
                        logger.info(f"Merged: {filename} → {os.path.basename(dest_folder)}/")
            logger.info("Folder merge completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error merging folders: {e}")
            return False
