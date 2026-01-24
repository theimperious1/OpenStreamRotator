import os
import re
import subprocess
import logging
import shutil
from typing import List, Dict, Optional
import sys
from core.database import DatabaseManager
from config.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class PlaylistManager:
    def __init__(self, db: DatabaseManager, config: ConfigManager):
        self.db = db
        self.config = config

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
    
    def select_playlists_for_rotation(self, manual_selection: Optional[List[str]] = None) -> List[Dict]:
        """
        Select playlists for the next rotation.
        Uses manual selection if provided, otherwise automatic selection.
        Only selects playlists that are defined in the config file.
        """
        # Get allowed playlist names from config
        config_playlists = self.config.get_playlists()
        allowed_names = {p['name'] for p in config_playlists if p.get('enabled', True)}
        
        if manual_selection:
            # Manual override mode
            all_playlists = self.db.get_enabled_playlists()
            selected = [p for p in all_playlists if p['name'] in manual_selection and p['name'] in allowed_names]
            logger.info(f"Manual selection: {[p['name'] for p in selected]}")
            return selected

        # Automatic selection
        settings = self.config.get_settings()
        min_playlists = settings.get('min_playlists_per_rotation', 2)
        max_playlists = settings.get('max_playlists_per_rotation', 4)

        all_playlists = self.db.get_enabled_playlists()
        
        # Filter to only include playlists in config
        all_playlists = [p for p in all_playlists if p['name'] in allowed_names]

        if len(all_playlists) == 0:
            logger.error("No enabled playlists available!")
            return []

        # Sort by last_played (oldest first) and priority
        # Playlists never played come first (NULLS FIRST is handled in SQL)

        # Select between min and max playlists
        num_to_select = min(len(all_playlists), max_playlists)
        num_to_select = max(num_to_select, min_playlists)

        selected = all_playlists[:num_to_select]

        logger.info(f"Auto-selected {len(selected)} playlists: {[p['name'] for p in selected]}")
        return selected

    def download_playlists(self, playlists: List[Dict], output_folder: str) -> bool:
        """
        Download selected playlists using yt-dlp.
        Returns True if successful, False otherwise.
        """
        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)

        settings = self.config.get_settings()
        max_retries = settings.get('download_retry_attempts', 3)

        all_success = True

        for playlist in playlists:
            playlist_name = playlist['name']
            playlist_url = playlist['youtube_url']

            logger.info(f"Downloading playlist: {playlist_name}")

            success = False
            for attempt in range(max_retries):
                try:
                    if self._download_single_playlist(playlist_url, output_folder):
                        success = True
                        logger.info(f"Successfully downloaded: {playlist_name}")

                        # Update database
                        self.db.update_playlist_played(playlist['id'])

                        # Register downloaded videos
                        self._register_downloaded_videos(playlist['id'], output_folder, playlist_name)
                        break
                    else:
                        logger.warning(f"Download attempt {attempt + 1} failed for {playlist_name}")
                except Exception as e:
                    logger.error(f"Error downloading {playlist_name}: {e}")

                if attempt < max_retries - 1:
                    logger.info(f"Retrying download for {playlist_name}...")

            if not success:
                logger.error(f"Failed to download {playlist_name} after {max_retries} attempts")
                all_success = False

        return all_success

    def _download_single_playlist(self, playlist_url: str, output_folder: str) -> bool:
        """Execute yt-dlp command to download a playlist."""
        cmd = [
            sys.executable,
            "-m", "yt_dlp",
            "--cookies-from-browser", "firefox",
            "--user-agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "--rm-cache-dir",
            "--retries", "10",
            "--fragment-retries", "10",
            "--ignore-errors",
            "--geo-bypass",
            "-o", f"{output_folder}/%(playlist)s_%(playlist_index)s_%(title)s.%(ext)s",
            playlist_url
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )

            if result.returncode == 0:
                return True
            else:
                logger.error(f"yt-dlp error: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error("Download timed out after 1 hour")
            return False
        except Exception as e:
            logger.error(f"Subprocess error: {e}")
            return False

    def _register_downloaded_videos(self, playlist_id: int, folder: str, playlist_name: str):
        """Register downloaded videos in the database."""
        video_extensions = ('.mp4', '.mkv', '.avi', '.webm', '.flv', '.mov')

        for filename in os.listdir(folder):
            if filename.lower().endswith(video_extensions):
                # Check if it belongs to this playlist
                if filename.startswith(playlist_name.replace(' ', '')):
                    file_path = os.path.join(folder, filename)
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

                    # Extract title from filename
                    title = self._extract_title_from_filename(filename)

                    self.db.add_video(
                        playlist_id=playlist_id,
                        filename=filename,
                        title=title,
                        file_size_mb=int(file_size_mb)
                    )

    def _extract_title_from_filename(self, filename: str) -> str:
        """Extract video title from filename."""
        # Remove extension
        name = os.path.splitext(filename)[0]

        # Pattern: PlaylistName_Index_Title
        match = re.match(r'^[^_]+_\d+_(.+)$', name)
        if match:
            return match.group(1)

        return name

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
            '24/7 @zackrawrr / @asmongold | {GAMES} | !playlist !streamtime !new'
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