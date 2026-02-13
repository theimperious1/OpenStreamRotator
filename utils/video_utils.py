"""
Shared video utility functions used across the codebase.

Centralizes operations like filename cleanup, category resolution,
and file extension checks to avoid duplication.
"""

import logging
import os
import re
from typing import Optional, TYPE_CHECKING

from config.constants import VIDEO_EXTENSIONS

if TYPE_CHECKING:
    from core.database import DatabaseManager
    from config.config_manager import ConfigManager

logger = logging.getLogger(__name__)

# Prefix pattern: "XX_" where XX is a 2-digit number
PREFIX_PATTERN = re.compile(r'^\d{2}_')


def strip_ordering_prefix(filename: str) -> str:
    """Strip the ordering prefix (e.g., '01_') from a video filename.

    Used to recover the original filename for database lookups,
    since videos are stored in the database without the prefix.

    Args:
        filename: Filename with optional ordering prefix (e.g., '01_CATS Being the Boss.webm')

    Returns:
        Original filename without prefix (e.g., 'CATS Being the Boss.webm')
    """
    return PREFIX_PATTERN.sub('', filename)


def is_video_file(filename: str) -> bool:
    """Check if a filename has a recognized video extension.

    Args:
        filename: Filename or path to check

    Returns:
        True if the file has a video extension
    """
    return os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS


def get_video_files_sorted(folder: str) -> list[str]:
    """Get sorted list of video files in a folder.

    Args:
        folder: Path to the folder to scan

    Returns:
        Alphabetically sorted list of video filenames
    """
    if not folder or not os.path.isdir(folder):
        return []
    return sorted([
        f for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f)) and is_video_file(f)
    ])


def resolve_playlist_categories(playlist: dict) -> dict[str, str]:
    """Resolve per-platform categories from a playlist config dict.

    Each playlist can have ``twitch_category`` and/or ``kick_category``.
    If only one is present it is used for both platforms.  Falls back to
    the legacy ``category`` field, then to the playlist name.

    Args:
        playlist: A single playlist dict from playlists.json

    Returns:
        ``{"twitch": "<cat>", "kick": "<cat>"}``
    """
    twitch = playlist.get("twitch_category") or playlist.get("category")
    kick = playlist.get("kick_category") or playlist.get("category")
    fallback = playlist.get("name", "Just Chatting")

    # When only one platform category is specified, share it
    if twitch and not kick:
        kick = twitch
    elif kick and not twitch:
        twitch = kick
    elif not twitch and not kick:
        twitch = kick = fallback

    return {"twitch": twitch, "kick": kick}


def resolve_category_for_video(
    video_filename: str,
    db: 'DatabaseManager',
    config: 'ConfigManager'
) -> Optional[dict[str, str]]:
    """Resolve per-platform stream categories for a video.

    Strips ordering prefix, looks up the original filename in the database,
    finds the source playlist, and returns the playlist's configured
    per-platform categories.

    Args:
        video_filename: Filename of the video (with or without ordering prefix)
        db: DatabaseManager instance for video lookup
        config: ConfigManager instance for playlist category lookup

    Returns:
        ``{"twitch": "...", "kick": "..."}`` or None if unable to determine
    """
    if not video_filename:
        return None

    try:
        # Strip ordering prefix (e.g., "01_") before DB lookup
        clean_filename = strip_ordering_prefix(video_filename)

        # Look up the video in database to find its source playlist
        video = db.get_video_by_filename(clean_filename)
        if not video:
            logger.debug(f"Video not found in database: {clean_filename}")
            return None

        playlist_name = video.get('playlist_name')
        if not playlist_name:
            logger.debug(f"No playlist_name for video: {video_filename}")
            return None

        # Get the category for this playlist from playlists config
        playlists_config = config.get_playlists()
        for p in playlists_config:
            if p.get('name') == playlist_name:
                return resolve_playlist_categories(p)

        logger.warning(f"Playlist '{playlist_name}' not found in config for video: {video_filename}")
        return None
    except Exception as e:
        logger.error(f"Error getting category for video {video_filename}: {e}")
        return None
