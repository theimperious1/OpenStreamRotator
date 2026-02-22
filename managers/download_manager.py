"""Background download manager with cross-thread DB queuing.

Owns the single-worker thread pool, download-in-progress flags,
and pending database operation queues that let the background
download thread communicate safely with the main SQLite thread.
"""
import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Callable, List, Optional

from config.config_manager import ConfigManager
from config.constants import DEFAULT_NEXT_ROTATION_FOLDER, DEFAULT_VIDEO_FOLDER
from core.database import DatabaseManager
from core.video_registration_queue import VideoRegistrationQueue
from managers.playlist_manager import PlaylistManager
from services.notification_service import NotificationService

logger = logging.getLogger(__name__)


class DownloadManager:

    def __init__(
        self,
        db: DatabaseManager,
        config_manager: ConfigManager,
        playlist_manager: PlaylistManager,
        notification_service: NotificationService,
        video_registration_queue: VideoRegistrationQueue,
        shutdown_event: Event,
    ):
        self.db = db
        self.config_manager = config_manager
        self.playlist_manager = playlist_manager
        self.notification_service = notification_service
        self.video_registration_queue = video_registration_queue
        self._shutdown_event = shutdown_event

        # Background thread pool (single worker so downloads are sequential)
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="download_worker")

        # Flags -----------------------------------------------------------
        self.background_download_in_progress = False
        self.downloads_triggered_this_rotation = False

        # Cross-thread DB queues (written by background thread, consumed by main)
        self._pending_db_playlists_to_initialize: Optional[List[str]] = None
        self._pending_db_playlists_to_complete: Optional[List[str]] = None

        # Callbacks set by the automation controller after construction
        self._get_current_session_id: Callable[[], Optional[int]] = lambda: None
        self._set_next_prepared_playlists: Callable = lambda v: None
        self._on_download_failure: Optional[Callable[[], None]] = None
        self._on_download_success: Optional[Callable[[], None]] = None

    # ------------------------------------------------------------------
    # Callback wiring (called once by AutomationController.__init__)
    # ------------------------------------------------------------------

    def set_callbacks(
        self,
        get_current_session_id: Callable[[], Optional[int]],
        set_next_prepared_playlists: Callable,
        on_download_failure: Optional[Callable[[], None]] = None,
        on_download_success: Optional[Callable[[], None]] = None,
    ) -> None:
        self._get_current_session_id = get_current_session_id
        self._set_next_prepared_playlists = set_next_prepared_playlists
        self._on_download_failure = on_download_failure
        self._on_download_success = on_download_success

    # ------------------------------------------------------------------
    # Background download trigger
    # ------------------------------------------------------------------

    async def trigger_next_rotation_async(self) -> None:
        """Select and start downloading the next rotation playlists.

        Called when temp playback exits to immediately prepare the next
        rotation instead of waiting for the current one to finish playing.
        """
        try:
            next_playlists = self.playlist_manager.select_playlists_for_rotation()

            if next_playlists:
                logger.info(
                    f"Auto-triggered next rotation selection after temp playback: "
                    f"{[p['name'] for p in next_playlists]}"
                )
                self.downloads_triggered_this_rotation = True
                self.background_download_in_progress = True
                loop = asyncio.get_event_loop()
                loop.run_in_executor(
                    self.executor,
                    self._sync_background_download,
                    next_playlists,
                )
            else:
                logger.warning("Failed to auto-select next rotation after temp playback")
        except Exception as e:
            logger.error(f"Error triggering next rotation after temp playback exit: {e}")

    def maybe_start_background_download(self, next_prepared_playlists) -> None:
        """Start a background download if conditions are met.

        Checks whether downloads should be triggered (no download in progress,
        no prepared playlists waiting, no backup folder content) and, if so,
        selects playlists and kicks off the download in the thread pool.

        Called from ``check_for_rotation`` in the main loop.
        """
        if self.background_download_in_progress or next_prepared_playlists is not None:
            return

        # Check if we have prepared content waiting in backup
        settings = self.config_manager.get_settings()
        base_path = os.path.dirname(settings.get('video_folder', DEFAULT_VIDEO_FOLDER))
        pending_backup_folder = os.path.normpath(os.path.join(base_path, 'temp_pending_backup'))

        if os.path.exists(pending_backup_folder) and os.listdir(pending_backup_folder):
            return

        # Select playlists in main thread (can't be done in executor thread due to SQLite)
        playlists = self.playlist_manager.select_playlists_for_rotation()
        if not playlists:
            return

        self.downloads_triggered_this_rotation = True
        self.background_download_in_progress = True
        loop = asyncio.get_event_loop()
        loop.run_in_executor(self.executor, self._sync_background_download, playlists)
        logger.debug("Download triggered (pending folder empty)")

    # ------------------------------------------------------------------
    # Synchronous download (runs in background thread)
    # ------------------------------------------------------------------

    def _sync_background_download(self, playlists) -> None:
        """Download playlists in the background thread.

        Must NOT call database methods directly — sets flags that the main
        thread processes via :meth:`process_pending_database_operations`.
        """
        try:
            logger.info(f"Downloading next rotation in thread: {[p['name'] for p in playlists]}")
            settings = self.config_manager.get_settings()
            next_folder = settings.get("next_rotation_folder", DEFAULT_NEXT_ROTATION_FOLDER)

            # Queue DB initialisation to be done in main thread
            self._pending_db_playlists_to_initialize = [p["name"] for p in playlists]

            verbose_download = settings.get("yt_dlp_verbose", False)
            download_result = self.playlist_manager.download_playlists(
                playlists, next_folder, verbose=verbose_download
            )

            if download_result.get("success"):
                self._set_next_prepared_playlists(playlists)
                logger.info(f"Background download completed: {[p['name'] for p in playlists]}")
                self._pending_db_playlists_to_complete = [p["name"] for p in playlists]
                self.notification_service.notify_next_rotation_ready([p["name"] for p in playlists])
                if self._on_download_success:
                    self._on_download_success()
            else:
                logger.warning("Background download had failures")
                self.notification_service.notify_background_download_warning()
                if self._on_download_failure:
                    self._on_download_failure()
        except Exception as e:
            logger.error(f"Background download error: {e}")
            self.notification_service.notify_background_download_error(str(e))
            if self._on_download_failure:
                self._on_download_failure()
        finally:
            self.background_download_in_progress = False

    # ------------------------------------------------------------------
    # Auto-resume interrupted downloads on startup
    # ------------------------------------------------------------------

    async def auto_resume_pending_downloads(self, session_id: int, pending_playlists: list) -> None:
        """Resume interrupted playlist downloads from a previous session.

        Uses yt-dlp's built-in ``--continue`` flag to pick up from partial
        ``.part`` files.
        """
        try:
            settings = self.config_manager.get_settings()
            next_folder = settings.get("next_rotation_folder", DEFAULT_NEXT_ROTATION_FOLDER)
            os.makedirs(next_folder, exist_ok=True)

            playlist_objects = self.db.get_playlists_with_ids_by_names(pending_playlists)
            if not playlist_objects:
                logger.warning(f"Could not fetch playlist objects for auto-resume: {pending_playlists}")
                return

            logger.info(f"Auto-resuming {len(playlist_objects)} interrupted playlist downloads on startup")

            def resume_downloads():
                try:
                    verbose_download = settings.get("yt_dlp_verbose", False)
                    result = self.playlist_manager.download_playlists(
                        playlist_objects, next_folder, verbose=verbose_download
                    )
                    if result.get("success"):
                        logger.info(f"Auto-resumed downloads completed for: {pending_playlists}")
                        for playlist in pending_playlists:
                            self.db.update_playlist_status(session_id, playlist, "COMPLETED")
                        self.background_download_in_progress = False
                    else:
                        logger.warning(f"Auto-resumed downloads had failures for: {pending_playlists}")
                        self.notification_service.notify_background_download_warning()
                        self.background_download_in_progress = False
                except Exception as e:
                    logger.error(f"Error during auto-resume of downloads: {e}")
                    self.notification_service.notify_background_download_error(str(e))
                    self.background_download_in_progress = False

            self.background_download_in_progress = True
            loop = asyncio.get_event_loop()
            loop.run_in_executor(self.executor, resume_downloads)
            logger.info("Auto-resume background task started")

        except Exception as e:
            logger.error(f"Failed to initiate auto-resume of pending downloads: {e}")

    # ------------------------------------------------------------------
    # Main-thread queue processors (called every tick from the main loop)
    # ------------------------------------------------------------------

    def process_video_registration_queue(self) -> None:
        """Register videos queued by the background download thread."""
        if not self.video_registration_queue.has_pending_videos():
            return

        pending_videos = self.video_registration_queue.get_pending_videos()
        if not pending_videos:
            return

        logger.info(f"Processing {len(pending_videos)} queued videos for database registration")

        registered_count = 0
        total_duration = 0

        for video_data in pending_videos:
            try:
                self.db.add_video(
                    video_data["playlist_id"],
                    video_data["filename"],
                    title=video_data["title"],
                    duration_seconds=video_data["duration_seconds"],
                    file_size_mb=video_data["file_size_mb"],
                    playlist_name=video_data.get("playlist_name"),
                )
                registered_count += 1
                total_duration += video_data["duration_seconds"]
                logger.debug(
                    f"Registered queued video: {video_data['filename']} ({video_data['duration_seconds']}s)"
                )
            except Exception as e:
                if "UNIQUE constraint failed" in str(e) or "already exists" in str(e):
                    logger.debug(f"Video already exists in database: {video_data['filename']}, skipping")
                else:
                    logger.error(f"Error registering queued video {video_data['filename']}: {e}")

        if registered_count > 0:
            logger.info(
                f"Registered {registered_count} queued videos from background download, total: {total_duration}s"
            )

    def process_pending_database_operations(self) -> None:
        """Apply database changes queued by the background download thread."""
        session_id = self._get_current_session_id()

        if self._pending_db_playlists_to_initialize is not None and session_id:
            self.db.initialize_next_playlists(session_id, self._pending_db_playlists_to_initialize)
            self._pending_db_playlists_to_initialize = None

        if self._pending_db_playlists_to_complete is not None and session_id:
            self.db.complete_next_playlists(session_id, self._pending_db_playlists_to_complete)
            self._pending_db_playlists_to_complete = None

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        """Stop the executor and wait briefly for in-flight downloads."""
        self.executor.shutdown(wait=False, cancel_futures=True)
        if self.background_download_in_progress:
            logger.info("Waiting up to 5s for download thread to notice shutdown...")
            self._shutdown_event.wait(5)
        logger.info("Thread executor shutdown complete")
