import json
import os
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, config_path: Optional[str] = None, settings_path: Optional[str] = None):
        config_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = config_path or os.path.join(config_dir, "playlists.json")
        self.settings_path = settings_path or os.path.join(config_dir, "settings.json")
        self.last_config_mtime = 0
        self.last_settings_mtime = 0

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
            "check_config_interval": 60,
            "min_playlists_per_rotation": 2,
            "max_playlists_per_rotation": 4,
            "download_retry_attempts": 3,
            "stream_title_template": "24/7 @example1 / @example2 | {GAMES} | !playlist !streamtime !new",
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
        """Get playlist configurations."""
        config = self.load_config()
        if config:
            return config.get('playlists', [])
        return []

    def get_settings(self) -> Dict:
        """Get application settings from settings.json.
        
        Folder paths (video_folder, next_rotation_folder) are read from
        environment variables (VIDEO_FOLDER, NEXT_ROTATION_FOLDER) and injected
        into the settings dict so all existing callers continue to work.
        
        All settings in settings.json are hot-swappable — the file is re-read
        on every call.
        """
        settings = self._load_json(self.settings_path) or {}

        # Inject env-var folder paths into settings dict (env overrides json fallback)
        settings['video_folder'] = os.getenv(
            'VIDEO_FOLDER',
            settings.get('video_folder', 'C:/stream_videos/')
        )
        settings['next_rotation_folder'] = os.getenv(
            'NEXT_ROTATION_FOLDER',
            settings.get('next_rotation_folder', 'C:/stream_videos_next/')
        )

        return settings

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