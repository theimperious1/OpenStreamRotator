import subprocess
import os
import logging
import re
import json

logger = logging.getLogger(__name__)


class VideoProcessor:
    """Handles video file operations and metadata extraction."""

    @staticmethod
    def get_video_duration(file_path: str) -> int:
        """
        Get video duration in seconds using ffprobe.
        
        Args:
            file_path: Path to video file
        
        Returns:
            Duration in seconds, or 0 if unable to determine
        """
        try:
            cmd = [
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'json',
                file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                duration_str = data.get('format', {}).get('duration', '0')
                duration = int(float(duration_str))
                logger.debug(f"Got duration for {os.path.basename(file_path)}: {duration}s")
                return duration
            else:
                logger.warning(f"ffprobe returned {result.returncode} for {os.path.basename(file_path)}")
                if result.stderr:
                    logger.warning(f"ffprobe stderr: {result.stderr[:200]}")
            return 0
        except subprocess.TimeoutExpired:
            logger.warning(f"Timeout getting duration for: {file_path}")
            return 0
        except FileNotFoundError:
            logger.warning("ffprobe not found. Install ffmpeg for video duration detection.")
            return 0
        except Exception as e:
            logger.warning(f"Error getting duration for {file_path}: {e}")
            return 0

    @staticmethod
    def extract_title_from_filename(filename: str) -> str:
        """
        Extract video title from filename (removes extensions and brackets).
        
        Args:
            filename: Video filename
        
        Returns:
            Extracted title
        """
        # Remove extension
        name_without_ext = os.path.splitext(filename)[0]
        
        # Remove common bracketed info like [720p] or (1080p)
        title = re.sub(r'\s*[\[\(][^\]\)]*[\]\)]', '', name_without_ext)
        
        # Clean up multiple spaces
        title = re.sub(r'\s+', ' ', title).strip()
        
        logger.debug(f"Extracted title from '{filename}': '{title}'")
        return title

    @staticmethod
    def get_supported_extensions() -> tuple:
        """Get tuple of supported video extensions."""
        return ('.mp4', '.mkv', '.avi', '.webm', '.flv', '.mov')

    @staticmethod
    def is_video_file(filename: str) -> bool:
        """Check if file has a supported video extension."""
        return filename.lower().endswith(VideoProcessor.get_supported_extensions())

    @staticmethod
    def get_video_files_in_folder(folder_path: str) -> list[str]:
        """
        Get sorted list of video files in a folder.
        
        Args:
            folder_path: Path to folder
        
        Returns:
            List of full paths to video files, sorted by filename
        """
        video_files = []
        
        if not os.path.exists(folder_path):
            logger.warning(f"Folder does not exist: {folder_path}")
            return video_files
        
        try:
            for filename in sorted(os.listdir(folder_path)):
                if VideoProcessor.is_video_file(filename):
                    full_path = os.path.abspath(os.path.join(folder_path, filename))
                    video_files.append(full_path)
        except Exception as e:
            logger.error(f"Error reading folder {folder_path}: {e}")
        
        return video_files
