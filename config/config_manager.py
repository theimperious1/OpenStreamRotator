import json
import os
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, config_path: str = None,
                 override_path: str = None):
        # Use config directory relative paths if not provided
        config_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = config_path or os.path.join(config_dir, "playlists.json")
        self.override_path = override_path or os.path.join(config_dir, "manual_override.json")
        self.last_config_mtime = 0
        self.last_override_mtime = 0

        # Create default config if doesn't exist
        if not os.path.exists(self.config_path):
            self.create_default_config()

        # Create default override if doesn't exist
        if not os.path.exists(self.override_path):
            self.create_default_override()

    def create_default_config(self):
        """Create a default configuration file."""
        default_config = {
            "playlists": [
                {
                    "name": "Example Playlist",
                    "url": "https://www.youtube.com/playlist?list=EXAMPLE",
                    "enabled": True,
                    "priority": 1
                }
            ],
            "settings": {
                "rotation_hours": 12,
                "video_folder": "C:/stream_videos/",
                "next_rotation_folder": "C:/stream_videos_next/",
                "check_config_interval": 60,
                "min_playlists_per_rotation": 2,
                "max_playlists_per_rotation": 4,
                "download_retry_attempts": 3,
                "stream_title_template": "24/7 @zackrawrr / @asmongold | {GAMES} | !playlist !streamtime !new"
            }
        }

        with open(self.config_path, 'w') as f:
            json.dump(default_config, f, indent=2)

        logger.info(f"Created default config at {self.config_path}")

    def create_default_override(self):
        """Create a default manual override file."""
        default_override = {
            "override_active": False,
            "selected_playlists": [],
            "trigger_now": False
        }

        with open(self.override_path, 'w') as f:
            json.dump(default_override, f, indent=2)

        logger.info(f"Created default override at {self.override_path}")

    def load_config(self) -> Dict | None:
        """Load configuration from file."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return None

    def load_override(self) -> Dict | None:
        """Load manual override from file."""
        try:
            with open(self.override_path, 'r') as f:
                override = json.load(f)
            return override
        except Exception as e:
            logger.error(f"Failed to load override: {e}")
            return None

    def has_config_changed(self) -> bool:
        """Check if config file has been modified."""
        try:
            current_mtime = os.path.getmtime(self.config_path)
            if current_mtime > self.last_config_mtime:
                self.last_config_mtime = current_mtime
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking config modification time: {e}")
            return False

    def has_override_changed(self) -> bool:
        """Check if override file has been modified."""
        try:
            current_mtime = os.path.getmtime(self.override_path)
            if current_mtime > self.last_override_mtime:
                self.last_override_mtime = current_mtime
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking override modification time: {e}")
            return False

    def get_playlists(self) -> List[Dict]:
        """Get playlist configurations."""
        config = self.load_config()
        if config:
            return config.get('playlists', [])
        return []

    def get_settings(self) -> Dict:
        """Get application settings."""
        config = self.load_config()
        if config:
            return config.get('settings', {})
        return {}

    def get_active_override(self) -> Optional[Dict]:
        """Get active manual override if exists."""
        override = self.load_override()
        if override and override.get('override_active', False):
            return override
        return None

    def clear_override(self):
        """Clear the manual override after it's been processed."""
        override = self.load_override()
        if override:
            override['override_active'] = False
            override['trigger_now'] = False
            override['selected_playlists'] = []

            with open(self.override_path, 'w') as f:
                json.dump(override, f, indent=2)

            logger.info("Manual override cleared")

    def validate_config(self) -> bool:
        """Validate configuration file structure."""
        config = self.load_config()
        if not config:
            return False

        # Check required fields
        if 'playlists' not in config or 'settings' not in config:
            logger.error("Config missing required fields: playlists or settings")
            return False

        # Validate playlists
        for playlist in config['playlists']:
            if 'name' not in playlist or 'url' not in playlist:
                logger.error(f"Invalid playlist config: {playlist}")
                return False

        # Validate settings
        settings = config['settings']
        required_settings = ['rotation_hours', 'video_folder', 'next_rotation_folder']
        for setting in required_settings:
            if setting not in settings:
                logger.error(f"Missing required setting: {setting}")
                return False

        return True