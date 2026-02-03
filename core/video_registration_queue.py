"""Thread-safe queue for video registration from background downloads."""
import queue
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class VideoRegistrationQueue:
    """Thread-safe queue for queueing videos to be registered in the database."""
    
    def __init__(self):
        """Initialize the video registration queue."""
        # Use thread-safe queue.Queue instead of regular lists
        self._queue: queue.Queue = queue.Queue()
    
    def enqueue_video(self, playlist_id: int, filename: str, title: str, 
                      duration_seconds: int, file_size_mb: int, playlist_name: Optional[str] = None):
        """
        Add a video to the registration queue.
        
        Thread-safe method that can be called from background threads.
        
        Args:
            playlist_id: Database playlist ID
            filename: Video filename
            title: Video title
            duration_seconds: Duration in seconds
            file_size_mb: File size in MB
            playlist_name: Name of the playlist from config (for category lookups)
        """
        video_data = {
            'playlist_id': playlist_id,
            'filename': filename,
            'title': title,
            'duration_seconds': duration_seconds,
            'file_size_mb': file_size_mb,
            'playlist_name': playlist_name
        }
        self._queue.put(video_data)
        logger.debug(f"Queued video for registration: {filename}")
    
    def get_pending_videos(self) -> list:
        """
        Get all pending videos from the queue.
        
        Returns a list of all queued video data and empties the queue.
        
        Returns:
            List of video data dictionaries
        """
        pending = []
        try:
            while True:
                video_data = self._queue.get_nowait()
                pending.append(video_data)
        except queue.Empty:
            pass
        
        return pending
    
    def has_pending_videos(self) -> bool:
        """Check if there are any pending videos in the queue."""
        return not self._queue.empty()
    
    def clear(self):
        """Clear all pending videos from the queue."""
        try:
            while True:
                self._queue.get_nowait()
        except queue.Empty:
            pass
        logger.debug("Video registration queue cleared")
