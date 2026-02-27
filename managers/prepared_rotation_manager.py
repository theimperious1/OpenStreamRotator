"""Prepared rotation management.

Allows users to pre-build named rotation sets with specific playlists,
download them on demand, and execute them later (immediately or on a schedule).

Lifecycle: created → downloading → ready → (scheduled) → executing → completed

Each prepared rotation lives in ``content/prepared/<slug>/`` with a
``metadata.json`` file tracking its state and a set of downloaded video files.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import time
from datetime import datetime, timezone
from threading import Event
from typing import TYPE_CHECKING, List, Optional

from config.constants import VIDEO_EXTENSIONS

if TYPE_CHECKING:
    from managers.download_manager import DownloadManager
    from managers.playlist_manager import PlaylistManager
    from config.config_manager import ConfigManager
    from core.video_registration_queue import VideoRegistrationQueue

logger = logging.getLogger(__name__)

# Base directory for prepared rotations (sibling of live/, pending/, etc.)
from config.constants import _PROJECT_ROOT
PREPARED_BASE = os.path.join(_PROJECT_ROOT, "content", "prepared")

# Valid status transitions
VALID_STATUSES = {"created", "downloading", "ready", "scheduled", "executing", "completed"}


def _slugify(title: str) -> str:
    """Turn a human title into a filesystem-safe folder name."""
    slug = re.sub(r"[^\w\s-]", "", title.lower().strip())
    slug = re.sub(r"[\s_-]+", "-", slug)
    return slug[:80] or "untitled"


def _unique_folder(title: str) -> str:
    """Return a unique folder path under PREPARED_BASE for the given title."""
    base = os.path.join(PREPARED_BASE, _slugify(title))
    if not os.path.exists(base):
        return base
    # Append numeric suffix to avoid collisions
    for i in range(2, 1000):
        candidate = f"{base}-{i}"
        if not os.path.exists(candidate):
            return candidate
    return f"{base}-{int(time.time())}"


class PreparedRotationManager:
    """Manages creation, download, execution, and cleanup of prepared rotations."""

    def __init__(
        self,
        playlist_manager: PlaylistManager,
        config_manager: ConfigManager,
        video_registration_queue: VideoRegistrationQueue,
        shutdown_event: Event,
    ):
        self.playlist_manager = playlist_manager
        self.config_manager = config_manager
        self.video_registration_queue = video_registration_queue
        self._shutdown_event = shutdown_event

        # Download manager is wired later to avoid circular imports
        self._download_manager: Optional[DownloadManager] = None

        # The currently executing prepared rotation folder (if any)
        self._executing_folder: Optional[str] = None

        os.makedirs(PREPARED_BASE, exist_ok=True)

        # On startup, reset any rotation left in "executing" state from a
        # previous crash / unclean shutdown back to "ready" so the user can
        # re-execute it from the dashboard.
        self._reset_stale_executing()

    def set_download_manager(self, dm: DownloadManager) -> None:
        self._download_manager = dm

    def resolve_folder(self, slug: str) -> Optional[str]:
        """Convert a slug to a validated absolute folder path.

        Returns the full path if the slug is safe and the folder exists,
        otherwise *None*.  Rejects any slug containing path separators,
        parent-directory references, or other traversal attempts.
        """
        if not slug or not isinstance(slug, str):
            return None
        # Reject anything that could escape PREPARED_BASE
        if any(c in slug for c in ("/", "\\", "\0")):
            logger.warning(f"Rejected slug with path separators: {slug!r}")
            return None
        if slug in (".", "..") or slug.startswith("."):
            logger.warning(f"Rejected slug with dot prefix: {slug!r}")
            return None
        folder = os.path.join(PREPARED_BASE, slug)
        # Double-check the resolved path is still under PREPARED_BASE
        resolved = os.path.realpath(folder)
        if not resolved.startswith(os.path.realpath(PREPARED_BASE)):
            logger.warning(f"Slug resolved outside PREPARED_BASE: {slug!r} -> {resolved}")
            return None
        if not os.path.isdir(folder):
            return None
        return folder

    def _reset_stale_executing(self) -> None:
        """Reset any rotations stuck in 'executing' from a previous run."""
        for entry in os.scandir(PREPARED_BASE):
            if not entry.is_dir():
                continue
            meta = self._read_meta(entry.path)
            if meta and meta.get("status") == "executing":
                logger.info(f"Resetting stale executing rotation to ready: {meta.get('title', entry.name)}")
                meta["status"] = "ready"
                self._write_meta(entry.path, meta)

    # ──────────────────────────────────────────────────────────────
    # Metadata helpers
    # ──────────────────────────────────────────────────────────────

    @staticmethod
    def _meta_path(folder: str) -> str:
        return os.path.join(folder, "metadata.json")

    @staticmethod
    def _read_meta(folder: str) -> Optional[dict]:
        path = os.path.join(folder, "metadata.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    @staticmethod
    def _write_meta(folder: str, meta: dict) -> None:
        path = os.path.join(folder, "metadata.json")
        os.makedirs(folder, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, default=str)

    @staticmethod
    def _count_videos(folder: str) -> int:
        if not os.path.exists(folder):
            return 0
        return sum(1 for f in os.listdir(folder) if f.lower().endswith(VIDEO_EXTENSIONS))

    # ──────────────────────────────────────────────────────────────
    # CRUD
    # ──────────────────────────────────────────────────────────────

    def create(self, title: str, playlist_names: List[str]) -> dict:
        """Create a new prepared rotation (no download yet).

        Returns the metadata dict including the ``folder`` key.
        """
        folder = _unique_folder(title)
        os.makedirs(folder, exist_ok=True)

        meta = {
            "title": title,
            "playlists": playlist_names,
            "status": "created",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "scheduled_at": None,
            "video_count": 0,
            "folder": folder,
            "is_fallback": False,
        }
        self._write_meta(folder, meta)
        logger.info(f"Created prepared rotation '{title}' at {folder}")
        return meta

    def list_all(self) -> List[dict]:
        """Return metadata for every prepared rotation, sorted by creation time."""
        results: List[dict] = []
        if not os.path.exists(PREPARED_BASE):
            return results

        for name in sorted(os.listdir(PREPARED_BASE)):
            folder = os.path.join(PREPARED_BASE, name)
            if not os.path.isdir(folder):
                continue
            meta = self._read_meta(folder)
            if meta:
                # Ensure folder is always up-to-date
                meta["folder"] = folder
                meta["video_count"] = self._count_videos(folder)
                results.append(meta)

        results.sort(key=lambda m: m.get("created_at", ""))
        return results

    def get(self, folder: str) -> Optional[dict]:
        """Get metadata for a specific prepared rotation by folder path."""
        meta = self._read_meta(folder)
        if meta:
            meta["folder"] = folder
            meta["video_count"] = self._count_videos(folder)
        return meta

    def delete(self, folder: str) -> bool:
        """Delete a prepared rotation (folder + contents).

        Cannot delete an executing rotation.
        """
        meta = self._read_meta(folder)
        if not meta:
            logger.warning(f"Cannot delete prepared rotation: metadata not found at {folder}")
            return False
        if meta.get("status") == "executing":
            logger.warning(f"Cannot delete executing prepared rotation: {meta.get('title')}")
            return False

        # If it's downloading, we can't truly cancel the thread, but we
        # remove the folder; the download thread will notice missing files.
        try:
            shutil.rmtree(folder)
            logger.info(f"Deleted prepared rotation '{meta.get('title')}' at {folder}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete prepared rotation at {folder}: {e}")
            return False

    def clear_completed(self) -> int:
        """Delete all completed prepared rotations. Returns count deleted."""
        count = 0
        for meta in self.list_all():
            if meta.get("status") == "completed":
                if self.delete(meta["folder"]):
                    count += 1
        return count

    # ──────────────────────────────────────────────────────────────
    # Download
    # ──────────────────────────────────────────────────────────────

    def is_any_downloading(self) -> bool:
        """Check if any prepared rotation is currently downloading."""
        for meta in self.list_all():
            if meta.get("status") == "downloading":
                return True
        return False

    def start_download(self, folder: str) -> bool:
        """Kick off a background download for a prepared rotation.

        Returns False if another download is already running or the
        rotation is not in 'created' status.
        """
        meta = self._read_meta(folder)
        if not meta:
            return False
        if meta["status"] != "created":
            logger.warning(f"Cannot download prepared rotation in status '{meta['status']}'")
            return False
        if self.is_any_downloading():
            logger.warning("Another prepared rotation is already downloading")
            return False
        if not self._download_manager:
            logger.error("Download manager not set — cannot download prepared rotation")
            return False

        # Resolve playlist objects from names
        playlists = self.playlist_manager.get_playlists_by_names(meta["playlists"])
        if not playlists:
            logger.warning(f"No matching playlists found for: {meta['playlists']}")
            return False

        meta["status"] = "downloading"
        self._write_meta(folder, meta)

        loop = asyncio.get_event_loop()
        loop.run_in_executor(
            self._download_manager.executor,
            self._sync_download,
            folder,
            playlists,
        )
        logger.info(f"Started downloading prepared rotation '{meta['title']}'")
        return True

    def _sync_download(self, folder: str, playlists: list) -> None:
        """Download playlists into the prepared rotation folder (runs in thread)."""
        try:
            settings = self.config_manager.get_settings()
            verbose = settings.get("yt_dlp_verbose", False)

            result = self.playlist_manager.download_playlists(playlists, folder, verbose=verbose)

            meta = self._read_meta(folder)
            if meta is None:
                # Folder was deleted while we were downloading
                logger.warning("Prepared rotation folder deleted during download")
                return

            if result.get("success"):
                meta["status"] = "ready"
                meta["video_count"] = self._count_videos(folder)
                logger.info(f"Prepared rotation '{meta['title']}' download complete — {meta['video_count']} videos")
            else:
                # Partial success — still mark as ready if we got some videos
                count = self._count_videos(folder)
                if count > 0:
                    meta["status"] = "ready"
                    meta["video_count"] = count
                    logger.warning(f"Prepared rotation '{meta['title']}' had download issues but {count} videos are available")
                else:
                    meta["status"] = "created"
                    logger.error(f"Prepared rotation '{meta['title']}' download failed — no videos")
            self._write_meta(folder, meta)
        except Exception as e:
            logger.error(f"Prepared rotation download error: {e}")
            meta = self._read_meta(folder)
            if meta:
                meta["status"] = "created"
                self._write_meta(folder, meta)

    def cancel_download(self, folder: str) -> bool:
        """Cancel a downloading prepared rotation by resetting its status.

        Note: The actual download thread cannot be killed, but it will
        complete into a folder the user can delete.
        """
        meta = self._read_meta(folder)
        if not meta or meta["status"] != "downloading":
            return False
        meta["status"] = "created"
        # Remove any partially downloaded videos
        for f in os.listdir(folder):
            if f != "metadata.json":
                try:
                    os.remove(os.path.join(folder, f))
                except Exception:
                    pass
        meta["video_count"] = 0
        self._write_meta(folder, meta)
        logger.info(f"Cancelled download for prepared rotation '{meta['title']}'")
        return True

    # ──────────────────────────────────────────────────────────────
    # Scheduling
    # ──────────────────────────────────────────────────────────────

    def schedule(self, folder: str, scheduled_at: str) -> bool:
        """Set a scheduled execution time for a prepared rotation.

        Args:
            scheduled_at: ISO 8601 datetime string (UTC).
        """
        meta = self._read_meta(folder)
        if not meta or meta["status"] != "ready":
            return False
        meta["status"] = "scheduled"
        meta["scheduled_at"] = scheduled_at
        self._write_meta(folder, meta)
        logger.info(f"Scheduled prepared rotation '{meta['title']}' for {scheduled_at}")
        return True

    def cancel_schedule(self, folder: str) -> bool:
        """Remove the scheduled time, returning to ready status."""
        meta = self._read_meta(folder)
        if not meta or meta["status"] != "scheduled":
            return False
        meta["status"] = "ready"
        meta["scheduled_at"] = None
        self._write_meta(folder, meta)
        logger.info(f"Cancelled schedule for prepared rotation '{meta['title']}'")
        return True

    def check_scheduled(self) -> Optional[str]:
        """Return the folder of a scheduled rotation whose time has arrived, or None."""
        now = datetime.now(timezone.utc)
        for meta in self.list_all():
            if meta.get("status") == "scheduled" and meta.get("scheduled_at"):
                try:
                    scheduled = datetime.fromisoformat(meta["scheduled_at"])
                    if scheduled.tzinfo is None:
                        scheduled = scheduled.replace(tzinfo=timezone.utc)
                    if scheduled <= now:
                        return meta["folder"]
                except (ValueError, TypeError):
                    continue
        return None

    # ──────────────────────────────────────────────────────────────
    # Execution
    # ──────────────────────────────────────────────────────────────

    def begin_execution(self, folder: str) -> Optional[dict]:
        """Mark a prepared rotation as executing and return its metadata.

        Returns None if the rotation can't be executed (wrong status, etc.).
        """
        meta = self._read_meta(folder)
        if not meta:
            return None
        if meta["status"] not in ("ready", "scheduled", "completed"):
            logger.warning(f"Cannot execute prepared rotation in status '{meta['status']}'")
            return None
        if self._count_videos(folder) == 0:
            logger.warning(f"Cannot execute prepared rotation '{meta['title']}' — no videos")
            return None

        meta["status"] = "executing"
        meta["scheduled_at"] = None
        self._write_meta(folder, meta)
        self._executing_folder = folder
        logger.info(f"Executing prepared rotation '{meta['title']}' ({meta.get('video_count', 0)} videos)")
        return meta

    def complete_execution(self, folder: Optional[str] = None) -> bool:
        """Mark the executing prepared rotation as completed."""
        target = folder or self._executing_folder
        if not target:
            return False
        meta = self._read_meta(target)
        if not meta:
            return False
        meta["status"] = "completed"
        self._write_meta(target, meta)
        self._executing_folder = None
        logger.info(f"Completed prepared rotation '{meta['title']}'")
        return True

    @property
    def executing_folder(self) -> Optional[str]:
        return self._executing_folder

    @property
    def is_executing(self) -> bool:
        return self._executing_folder is not None

    def get_executing(self) -> Optional[dict]:
        """Get metadata of the currently executing prepared rotation."""
        if not self._executing_folder:
            return None
        return self.get(self._executing_folder)

    # ──────────────────────────────────────────────────────────────
    # Fallback
    # ──────────────────────────────────────────────────────────────

    def set_fallback(self, folder: str, value: bool) -> bool:
        """Mark or unmark a prepared rotation as fallback content."""
        meta = self._read_meta(folder)
        if not meta:
            return False
        meta["is_fallback"] = value
        self._write_meta(folder, meta)
        logger.info(f"Prepared rotation '{meta.get('title')}' is_fallback set to {value}")
        return True

    def get_fallback_rotation(self) -> Optional[str]:
        """Return the folder of a ready fallback rotation, or None.

        Picks the first fallback-marked rotation in ``ready`` or
        ``completed`` status (sorted by creation time).
        """
        for meta in self.list_all():
            if (meta.get("is_fallback")
                    and meta.get("status") in ("ready", "completed")
                    and self._count_videos(meta["folder"]) > 0):
                return meta["folder"]
        return None

    def has_fallback_content(self) -> bool:
        """Return True if at least one fallback rotation is ready."""
        return self.get_fallback_rotation() is not None

    # ──────────────────────────────────────────────────────────────
    # State snapshot for dashboard
    # ──────────────────────────────────────────────────────────────

    def get_dashboard_state(self) -> dict:
        """Return a state dict suitable for the web dashboard.

        Emits *slug* (the bare directory name) instead of full filesystem
        paths so that absolute paths are never exposed over the wire.
        """
        rotations = []
        for meta in self.list_all():
            rotations.append({
                "slug": os.path.basename(meta["folder"]),
                "title": meta.get("title", "Untitled"),
                "playlists": meta.get("playlists", []),
                "status": meta.get("status", "created"),
                "video_count": meta.get("video_count", 0),
                "created_at": meta.get("created_at"),
                "scheduled_at": meta.get("scheduled_at"),
                "is_fallback": meta.get("is_fallback", False),
            })
        return {
            "prepared_rotations": rotations,
            "any_downloading": self.is_any_downloading(),
            "executing_slug": os.path.basename(self._executing_folder) if self._executing_folder else None,
        }
