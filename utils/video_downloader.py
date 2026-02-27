"""YouTube playlist downloader via yt-dlp.

Downloads playlists with per-video retry logic, post-processing
hooks for duration extraction, and archive-based deduplication.
"""
import os
import logging
from threading import Event, Lock
from typing import List, Dict, Optional, Set
from yt_dlp import YoutubeDL
from core.database import DatabaseManager
from config.config_manager import ConfigManager
from utils.video_processor import VideoProcessor
from core.video_registration_queue import VideoRegistrationQueue

logger = logging.getLogger(__name__)


class VideoDownloader:

    def __init__(self, db: DatabaseManager, config: ConfigManager, 
                 registration_queue: Optional[VideoRegistrationQueue] = None,
                 shutdown_event: Optional[Event] = None):
        """
        Initialize video downloader.
        
        Args:
            db: DatabaseManager instance
            config: ConfigManager instance
            registration_queue: VideoRegistrationQueue for thread-safe video registration
            shutdown_event: Threading event set when the application is shutting down
        """
        self.db = db
        self.config = config
        self.registration_queue = registration_queue
        self.shutdown_event = shutdown_event or Event()
        
        # Thread-safe set of filenames already registered via per-video post_hooks
        # Prevents _register_downloaded_videos from re-registering under wrong playlist
        self._registered_files: Set[str] = set()
        self._registered_files_lock = Lock()
        
        # Force yt-dlp to use Deno instead of Node.js by removing Node.js from PATH
        # This prevents conflicts between Node.js and Deno JavaScript runtimes
        if 'PATH' in os.environ:
            path_parts = os.environ['PATH'].split(';')
            # Filter out Node.js installation directory
            path_parts = [p for p in path_parts if 'nodejs' not in p.lower()]
            os.environ['PATH'] = ';'.join(path_parts)
            logger.debug("Removed Node.js from PATH to force yt-dlp to use Deno")

    def download_playlists(self, playlists: List[Dict], output_folder: str, verbose: bool = False) -> Dict:
        """
        Download selected playlists using yt-dlp.
        
        Returns dict with 'success' bool and 'total_duration_seconds' from all videos.
        
        Args:
            playlists: List of playlist dictionaries with 'youtube_url' key
            output_folder: Folder to download videos to
            verbose: If True, enable verbose yt-dlp logging for debugging
        
        Returns:
            Dict with keys: success (bool), total_duration_seconds (int)
        """
        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)
        
        # Explicitly create temp folder for yt-dlp's temporary files
        # yt-dlp will use this with 'paths' config for fragments, .part, and .ytdl files
        temp_folder = os.path.join(output_folder, 'temp')
        os.makedirs(temp_folder, exist_ok=True)

        settings = self.config.get_settings()
        max_retries = settings.get('download_retry_attempts', 3)

        all_success = True
        total_duration = 0

        for playlist in playlists:
            if self.shutdown_event.is_set():
                logger.info("Shutdown requested, aborting remaining playlist downloads")
                return {'success': False, 'total_duration_seconds': total_duration}

            # Snapshot files before this playlist starts — batch registration will
            # only process files that appeared after this point, preventing
            # cross-playlist contamination when multiple playlists share a folder.
            pre_existing_files = set(os.listdir(output_folder)) if os.path.exists(output_folder) else set()

            result = self._download_single_playlist(
                playlist['youtube_url'], output_folder, max_retries, verbose=verbose,
                playlist_id=playlist['id'], playlist_name=playlist['name']
            )
            
            if result.get('success'):
                # Register downloaded videos in database
                duration = self._register_downloaded_videos(
                    playlist['id'],
                    output_folder,
                    playlist['name'],
                    pre_existing_files=pre_existing_files
                )
                total_duration += duration
            else:
                all_success = False
                logger.warning(f"Failed to download playlist: {playlist['name']}")

        # Final sanity check: if yt-dlp reported success for every playlist but
        # zero video files actually landed (e.g. ignoreerrors skipped all videos
        # due to network failure), treat the whole download as a failure.
        if all_success and not VideoProcessor.get_video_files_in_folder(output_folder):
            logger.warning("All playlists reported success but no video files were downloaded — treating as failure")
            all_success = False

        return {
            'success': all_success,
            'total_duration_seconds': total_duration
        }

    def _download_single_playlist(self, playlist_url: str, output_folder: str, max_retries: int = 3,
                                   verbose: bool = False, playlist_id: Optional[int] = None,
                                   playlist_name: Optional[str] = None) -> Dict:
        """
        Download a single YouTube playlist using yt-dlp library.
        
        Args:
            playlist_url: YouTube playlist URL
            output_folder: Folder to download to
            max_retries: Maximum retry attempts
            verbose: If True, log full yt-dlp output for debugging
            playlist_id: Database playlist ID for per-video registration
            playlist_name: Playlist name for per-video registration
        
        Returns:
            Dict with 'success' key (bool)
        """
        for attempt in range(max_retries):
            if self.shutdown_event.is_set():
                logger.info("Shutdown requested, aborting download retries")
                return {'success': False}

            try:
                logger.info(f"Downloading playlist (attempt {attempt + 1}/{max_retries}): {playlist_url}")
                
                # Configure yt-dlp options
                
                # Per-video registration hook: queue each video for DB registration
                # immediately after yt-dlp finishes it, so it's available for category
                # lookups even if the playlist download is still in progress
                def _on_video_complete(filepath: str) -> None:
                    if not os.path.exists(filepath):
                        return
                    filename = os.path.basename(filepath)
                    if not VideoProcessor.is_video_file(filename):
                        return
                    try:
                        if not VideoProcessor.has_valid_video_stream(filepath):
                            return
                        title = VideoProcessor.extract_title_from_filename(filename)
                        duration = VideoProcessor.get_video_duration(filepath) or 0
                        file_size_mb = int(os.path.getsize(filepath) / (1024 * 1024))
                        if self.registration_queue and playlist_id is not None:
                            self.registration_queue.enqueue_video(
                                playlist_id, filename,
                                title=title, duration_seconds=duration,
                                file_size_mb=file_size_mb, playlist_name=playlist_name
                            )
                            with self._registered_files_lock:
                                self._registered_files.add(filename)
                            logger.debug(f"Per-video registration queued: {filename} ({duration}s) for {playlist_name}")
                    except Exception as e:
                        logger.debug(f"Per-video registration failed for {filename}: {e}")

                ydl_opts = {
                    'quiet': not verbose,
                    'no_warnings': not verbose,
                    'ignoreerrors': True,  # Skip unavailable/private videos after retries instead of failing entire playlist
                    'extractor_retries': 3,  # Retry extraction errors (private/unavailable) 3 times per video before skipping
                    'retries': 3,  # Retry download HTTP errors 3 times per video before skipping
                    'extract_flat': False,  # Extract video URLs
                    'fragment_retries': 3,
                    'concurrent_fragment_downloads': 5,  # Download 4 fragments in parallel
                    'http_chunk_size': 10485760,  # 10MB chunks - I tried raising this higher, doesn't work
                    'outtmpl': '%(playlist)s_%(playlist_index)03d_%(title)s.%(ext)s',
                    'post_hooks': [_on_video_complete],
                    # Archive file to track downloaded videos - prevents re-downloading
                    # videos that were deleted during temp playback
                    # Archive file tracks downloaded video IDs to prevent re-downloading
                    # during temp playback when videos are deleted after being played
                    'download_archive': os.path.join(output_folder, 'archive.txt'),
                    # Separate temp files (fragments, .part, .ytdl) into temp/ subfolder
                    # Final completed videos stay in output_folder, temps in output_folder/temp/
                    'paths': {
                        'home': output_folder,
                        'temp': os.path.join(output_folder, 'temp'),
                    },
                    # Request throttling to avoid YouTube IP-based blocking
                    'socket_timeout': 30,
                    'sleep_interval': 2,  # Sleep 2 seconds between requests
                    'max_sleep_interval': 5,  # Randomize sleep up to 5 seconds
                    'sleep_interval_requests': 1,  # Sleep after every request
                    # 'ratelimit': 50000000,  # 50MB/s rate limit for balanced speed
                    # Write info.json for metadata-aware resumption of interrupted downloads
                    'write_info_json': True,
                    # Auto-download JS challenge solver from GitHub (required for YouTube signatures)
                    'remote_components': ['ejs:github'],
                    # 'extractor_args': {
                        # 'youtube': {
                            # Use ios_downgraded to avoid YouTube's aggressive IP-based blocking
                            # Mobile clients have different detection patterns, reducing blocks
                            # 'player_client': ['ios_downgraded', 'default', '-android_sdkless'],
                        # }
                    # },
                }
                
                # Add cookie support for age-restricted videos if enabled
                # Read from playlists.json settings (hot-swappable mid-retry)
                cookie_settings = self.config.get_settings()
                use_cookies = cookie_settings.get('yt_dlp_use_cookies', False)
                if use_cookies:
                    browser = str(cookie_settings.get('yt_dlp_browser_for_cookies', 'firefox')).lower()
                    ydl_opts['cookiesfrombrowser'] = (browser,)
                    logger.debug(f"Using cookies from browser: {browser}")
                
                if verbose:
                    logger.debug(f"yt-dlp options (attempt {attempt + 1}): {ydl_opts}")
                
                # Exponential backoff on retries (interruptible by shutdown)
                if attempt > 0:
                    wait_time = min(2 ** (attempt - 1), 8)  # 1s, 2s, 4s, 8s
                    logger.debug(f"Waiting {wait_time}s before retry...")
                    if self.shutdown_event.wait(wait_time):
                        logger.info("Shutdown requested during backoff, aborting")
                        return {'success': False}
                
                # Download using yt-dlp library directly
                with YoutubeDL(ydl_opts) as ydl:  # type: ignore
                    ydl.extract_info(playlist_url, download=True)
                
                logger.info(f"Successfully downloaded playlist: {playlist_url}")
                return {'success': True}
                
            except Exception as e:
                error_msg = str(e)
                if 'Requested format is not available' in error_msg:
                    logger.warning(f"Format not available - YouTube blocking (attempt {attempt + 1}/{max_retries}): {error_msg[:200]}")
                else:
                    logger.warning(f"Download error (attempt {attempt + 1}/{max_retries}): {error_msg[:200]}")

        logger.error(f"Failed to download playlist after {max_retries} attempts: {playlist_url}")
        return {'success': False}

    def _register_downloaded_videos(self, playlist_id: int, folder: str, playlist_name: str,
                                      pre_existing_files: Optional[Set[str]] = None) -> int:
        """
        Register newly downloaded videos in the queue for later database insertion.
        
        This method queues videos instead of writing directly to the database,
        allowing background downloads to avoid thread safety issues.
        
        Args:
            playlist_id: Database playlist ID
            folder: Folder containing downloaded videos
            playlist_name: Name of playlist for logging
            pre_existing_files: Set of filenames that existed before this playlist's
                download started. Files in this set are skipped to prevent
                cross-playlist contamination.
        
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

            # Skip files that existed before this playlist's download started —
            # they belong to a different playlist and must not be re-registered
            # under this one (prevents cross-playlist prefix misassignment)
            if pre_existing_files is not None and filename in pre_existing_files:
                continue

            # Skip files already registered via per-video post_hooks (prevents
            # cross-playlist registration when multiple playlists share a folder)
            with self._registered_files_lock:
                if filename in self._registered_files:
                    logger.debug(f"Already registered via post_hook, skipping batch: {filename}")
                    continue

            # Also check the database — after a restart _registered_files is empty,
            # but files from other playlists may already be registered in the DB.
            # Without this check, batch registration would assign them to the wrong playlist.
            existing = self.db.get_video_by_filename(filename)
            if existing:
                logger.debug(f"Already in database (playlist={existing.get('playlist_name')}), skipping batch: {filename}")
                continue

            # Validate video stream before processing (ensures file is complete, not still post-processing)
            if not VideoProcessor.has_valid_video_stream(video_path):
                logger.warning(f"Video stream validation failed for {filename}, skipping registration")
                continue

            # Get video metadata
            title = VideoProcessor.extract_title_from_filename(filename)
            duration = VideoProcessor.get_video_duration(video_path)
            
            # Handle None duration (video processing failed)
            if duration is None:
                logger.warning(f"Failed to get duration for {filename}, using 0")
                duration = 0
            
            file_size_mb = os.path.getsize(video_path) / (1024 * 1024)

            # If queue available, queue video instead of writing directly
            if self.registration_queue:
                try:
                    self.registration_queue.enqueue_video(
                        playlist_id,
                        filename,
                        title=title,
                        duration_seconds=duration,
                        file_size_mb=int(file_size_mb),
                        playlist_name=playlist_name
                    )
                    # Track as registered so subsequent playlist batch scans
                    # don't re-queue the same file under a different playlist
                    with self._registered_files_lock:
                        self._registered_files.add(filename)
                    total_duration += duration
                    registered_count += 1
                    logger.debug(f"Queued video for registration: {filename} ({duration}s)")
                except Exception as e:
                    logger.error(f"Error queueing video {filename}: {e}")
            else:
                # Fallback: write directly to database if no queue (shouldn't happen in normal operation)
                try:
                    self.db.add_video(
                        playlist_id,
                        filename,
                        title=title,
                        duration_seconds=duration,
                        file_size_mb=int(file_size_mb),
                        playlist_name=playlist_name
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
        
        logger.info(f"Queued {registered_count} new videos for {playlist_name}, total: {total_duration}s")
        return total_duration
