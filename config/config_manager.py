"""Configuration manager for playlists and application settings.

Loads and saves playlists.json and settings.json with mtime-based
change detection for live-reload in the main loop.
"""
import json
import os
import logging
from typing import Dict, List, Optional

from config.constants import DEFAULT_VIDEO_FOLDER, DEFAULT_NEXT_ROTATION_FOLDER

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, config_path: Optional[str] = None, settings_path: Optional[str] = None):
        config_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = config_path or os.path.join(config_dir, "playlists.json")
        self.settings_path = settings_path or os.path.join(config_dir, "settings.json")
        # Seed with actual mtimes so the first has_config_changed() call
        # doesn't spuriously report a change on startup.
        self.last_config_mtime: float = self._safe_mtime(self.config_path)
        self.last_settings_mtime: float = self._safe_mtime(self.settings_path)
        # Mtime-based caches — re-read only when file changes on disk
        self._cached_settings: Optional[Dict] = None
        self._cached_playlists: Optional[List[Dict]] = None
        self._settings_cache_mtime: float = 0
        self._playlists_cache_mtime: float = 0

        # Create default files if they don't exist
        if not os.path.exists(self.config_path):
            self._create_default_playlists()
        if not os.path.exists(self.settings_path):
            self._create_default_settings()

    def _create_default_playlists(self):
        """Create a default playlists configuration file."""
        default_playlists = {
            "playlists": [
                {
                    "name": "Example Playlist",
                    "url": "https://www.youtube.com/playlist?list=EXAMPLE",
                    "enabled": True,
                    "priority": 1
                }
            ]
        }

        with open(self.config_path, 'w') as f:
            json.dump(default_playlists, f, indent=2)

        logger.info(f"Created default playlists config at {self.config_path}")

    def _create_default_settings(self):
        """Create a default settings configuration file."""
        default_settings = {
            "min_playlists_per_rotation": 2,
            "max_playlists_per_rotation": 4,
            "download_retry_attempts": 3,
            "stream_title_template": "My OpenStreamRotator 24/7 channel! | {GAMES} | lorem epsum",
            "debug_mode": False,
            "yt_dlp_use_cookies": False,
            "yt_dlp_browser_for_cookies": "firefox"
        }

        with open(self.settings_path, 'w') as f:
            json.dump(default_settings, f, indent=2)

        logger.info(f"Created default settings at {self.settings_path}")

    # Keep legacy method name for any external callers
    def create_default_config(self):
        """Create default configuration files."""
        self._create_default_playlists()
        self._create_default_settings()

    def _load_json(self, path: str) -> Dict | None:
        """Load a JSON file and return its contents."""
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load {path}: {e}")
            return None

    def load_config(self) -> Dict | None:
        """Load playlist configuration from file."""
        return self._load_json(self.config_path)

    def has_config_changed(self) -> bool:
        """Check if either config file has been modified."""
        changed = False
        try:
            current_mtime = os.path.getmtime(self.config_path)
            if current_mtime > self.last_config_mtime:
                self.last_config_mtime = current_mtime
                changed = True
        except Exception as e:
            logger.error(f"Error checking playlist config modification time: {e}")

        try:
            current_mtime = os.path.getmtime(self.settings_path)
            if current_mtime > self.last_settings_mtime:
                self.last_settings_mtime = current_mtime
                changed = True
        except Exception as e:
            logger.error(f"Error checking settings modification time: {e}")

        return changed

    def get_playlists(self) -> List[Dict]:
        """Get playlist configurations (cached, re-read on file change)."""
        try:
            current_mtime = os.path.getmtime(self.config_path)
        except OSError:
            current_mtime = 0

        if self._cached_playlists is not None and current_mtime == self._playlists_cache_mtime:
            return self._cached_playlists

        self._playlists_cache_mtime = current_mtime
        config = self.load_config()
        self._cached_playlists = config.get('playlists', []) if config else []
        return self._cached_playlists

    def get_settings(self) -> Dict:
        """Get application settings from settings.json (cached, re-read on file change).
        
        Folder paths (video_folder, next_rotation_folder) are read from
        environment variables (VIDEO_FOLDER, NEXT_ROTATION_FOLDER) and injected
        into the settings dict so all existing callers continue to work.
        
        All settings in settings.json are hot-swappable — the cache is
        invalidated whenever the file's mtime changes.
        """
        try:
            current_mtime = os.path.getmtime(self.settings_path)
        except OSError:
            current_mtime = 0

        if self._cached_settings is not None and current_mtime == self._settings_cache_mtime:
            return self._cached_settings

        self._settings_cache_mtime = current_mtime
        settings = self._load_json(self.settings_path) or {}

        # Inject env-var folder paths into settings dict (env overrides json fallback)
        settings['video_folder'] = os.getenv(
            'VIDEO_FOLDER',
            settings.get('video_folder', DEFAULT_VIDEO_FOLDER)
        )
        settings['next_rotation_folder'] = os.getenv(
            'NEXT_ROTATION_FOLDER',
            settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
        )

        self._cached_settings = settings
        return settings

    @property
    def video_folder(self) -> str:
        """Get the live video folder path."""
        return self.get_settings().get('video_folder', DEFAULT_VIDEO_FOLDER)

    @property
    def next_rotation_folder(self) -> str:
        """Get the pending/next rotation folder path."""
        return self.get_settings().get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)

    def validate_config(self) -> bool:
        """Validate configuration file structures."""
        config = self.load_config()
        if not config:
            return False

        if 'playlists' not in config:
            logger.error("Playlists config missing required field: playlists")
            return False

        for playlist in config['playlists']:
            if 'name' not in playlist or 'url' not in playlist:
                logger.error(f"Invalid playlist config: {playlist}")
                return False

        settings = self._load_json(self.settings_path)
        if not settings:
            logger.error("Failed to load settings.json")
            return False

        return True
    
    @staticmethod
    def _safe_mtime(path: str) -> float:
        """Return the file's mtime, or 0 if it doesn't exist yet."""
        try:
            return os.path.getmtime(path)
        except OSError:
            return 0
        