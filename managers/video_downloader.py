import os
import subprocess
import sys
import logging
from typing import List, Dict
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from services.video_processor import VideoProcessor

logger = logging.getLogger(__name__)


class VideoDownloader:
    """Handles YouTube playlist downloading and video registration."""

    def __init__(self, db: DatabaseManager, config: ConfigManager):
        """
        Initialize video downloader.
        
        Args:
            db: DatabaseManager instance
            config: ConfigManager instance
        """
        self.db = db
        self.config = config

    def download_playlists(self, playlists: List[Dict], output_folder: str) -> Dict:
        """
        Download selected playlists using yt-dlp.
        
        Returns dict with 'success' bool and 'total_duration_seconds' from all videos.
        
        Args:
            playlists: List of playlist dictionaries with 'youtube_url' key
            output_folder: Folder to download videos to
        
        Returns:
            Dict with keys: success (bool), total_duration_seconds (int)
        """
        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)

        settings = self.config.get_settings()
        max_retries = settings.get('download_retry_attempts', 3)

        all_success = True
        total_duration = 0

        for playlist in playlists:
            result = self._download_single_playlist(playlist['youtube_url'], output_folder, max_retries)
            
            if result.get('success'):
                # Register downloaded videos in database
                duration = self._register_downloaded_videos(
                    playlist['id'],
                    output_folder,
                    playlist['name']
                )
                total_duration += duration
            else:
                all_success = False
                logger.warning(f"Failed to download playlist: {playlist['name']}")

        return {
            'success': all_success,
            'total_duration_seconds': total_duration
        }

    def _download_single_playlist(self, playlist_url: str, output_folder: str, max_retries: int = 3) -> Dict:
        """
        Download a single YouTube playlist using yt-dlp.
        
        Args:
            playlist_url: YouTube playlist URL
            output_folder: Folder to download to
            max_retries: Maximum retry attempts
        
        Returns:
            Dict with 'success' key (bool)
        """
        for attempt in range(max_retries):
            try:
                # yt-dlp download command - run as Python module
                cmd = [
                    sys.executable,
                    "-m", "yt_dlp",
                    "--no-warnings",
                    "-q",
                    "-o", os.path.join(output_folder, '%(title)s.%(ext)s'),
                    playlist_url
                ]

                logger.info(f"Downloading playlist (attempt {attempt + 1}/{max_retries}): {playlist_url}")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

                if result.returncode == 0:
                    logger.info(f"Successfully downloaded playlist: {playlist_url}")
                    return {'success': True}
                else:
                    logger.warning(f"yt-dlp returned code {result.returncode}")
                    if result.stderr:
                        logger.warning(f"yt-dlp stderr: {result.stderr[:200]}")

            except subprocess.TimeoutExpired:
                logger.warning(f"Download timeout (attempt {attempt + 1}/{max_retries})")
            except FileNotFoundError:
                logger.error("yt-dlp not found. Install it with: pip install yt-dlp")
                return {'success': False}
            except Exception as e:
                logger.warning(f"Download error (attempt {attempt + 1}/{max_retries}): {e}")

        logger.error(f"Failed to download playlist after {max_retries} attempts: {playlist_url}")
        return {'success': False}

    def _register_downloaded_videos(self, playlist_id: int, folder: str, playlist_name: str) -> int:
        """
        Register newly downloaded videos in the database.
        
        Args:
            playlist_id: Database playlist ID
            folder: Folder containing downloaded videos
            playlist_name: Name of playlist for logging
        
        Returns:
            Total duration of newly registered videos in seconds
        """
        video_files = VideoProcessor.get_video_files_in_folder(folder)
        
        if not video_files:
            logger.warning(f"No video files found in {folder}")
            return 0

        total_duration = 0
        registered_count = 0

        for video_path in video_files:
            filename = os.path.basename(video_path)

            # Get video metadata
            title = VideoProcessor.extract_title_from_filename(filename)
            duration = VideoProcessor.get_video_duration(video_path)
            file_size_mb = os.path.getsize(video_path) / (1024 * 1024)

            # Try to add video, skip if already exists
            try:
                self.db.add_video(
                    playlist_id,
                    filename,
                    title=title,
                    duration_seconds=duration,
                    file_size_mb=int(file_size_mb)
                )
                total_duration += duration
                registered_count += 1
                logger.debug(f"Registered video: {filename} ({duration}s)")
            except Exception as e:
                # Check if it's a duplicate constraint error
                if "UNIQUE constraint failed" in str(e) or "already exists" in str(e):
                    logger.debug(f"Video already exists: {filename}, skipping")
                else:
                    logger.error(f"Error registering video {filename}: {e}")

        logger.info(f"Registered {registered_count} new videos for {playlist_name}, total: {total_duration}s")
        return total_duration
