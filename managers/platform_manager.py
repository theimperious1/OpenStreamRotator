import logging
from typing import Optional, List
from integrations.platforms.base.stream_platform import StreamPlatform
from integrations.platforms.twitch import TwitchUpdater
from integrations.platforms.kick import KickUpdater

logger = logging.getLogger(__name__)

class PlatformManager:
    """Manages multiple streaming platform integrations."""

    def __init__(self):
        self.platforms: List[StreamPlatform] = []
        self.enabled_platforms = set()

    def add_twitch(self, client_id: str, access_token: str, broadcaster_id: str):
        """Add Twitch platform integration."""
        try:
            twitch = TwitchUpdater(client_id, access_token, broadcaster_id)
            self.platforms.append(twitch)
            self.enabled_platforms.add("twitch")
            logger.info("Twitch integration enabled")
            return twitch
        except Exception as e:
            logger.error(f"Failed to enable Twitch: {e}")
            return None

    def add_kick(self, client_id: str, client_secret: str, channel_id: str,
                 redirect_uri: str = "http://localhost:8080/callback"):
        """Add Kick platform integration."""
        try:
            kick = KickUpdater(client_id, client_secret, channel_id, redirect_uri)
            self.platforms.append(kick)
            self.enabled_platforms.add("kick")
            logger.info("Kick integration enabled")
            return kick
        except ImportError as e:
            logger.warning(f"Kick integration not available: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to enable Kick: {e}")
            return None

    def get_platform(self, platform_name: str) -> Optional[StreamPlatform]:
        """Get a specific platform by name."""
        for platform in self.platforms:
            if platform.platform_name.lower() == platform_name.lower():
                return platform
        return None

    async def update_title_all(self, title: str) -> dict[str, bool]:
        """Update title on all enabled platforms."""
        results = {}
        for platform in self.platforms:
            results[platform.platform_name] = await platform.update_title(title)
        return results

    def update_category_all(self, category: str) -> dict[str, bool]:
        """Update category on all enabled platforms."""
        results = {}
        for platform in self.platforms:
            results[platform.platform_name] = platform.update_category(category)
        return results

    async def update_stream_info_all(self, title: str, category: Optional[str] = None) -> dict[str, bool]:
         """Update stream title and category on all enabled platforms."""
         results = {}
         for platform in self.platforms:
             results[platform.platform_name] = await platform.update_stream_info(title, category)
         return results

    def is_platform_enabled(self, platform_name: str) -> bool:
        """Check if a platform is enabled."""
        return platform_name.lower() in self.enabled_platforms

    def get_enabled_platforms(self) -> List[str]:
        """Get list of enabled platform names."""
        return [p.platform_name for p in self.platforms]

    def cleanup(self):
        """Clean up all platform resources."""
        for platform in self.platforms:
            if hasattr(platform, 'close'):
                try:
                    platform.close()  # type: ignore
                except Exception as e:
                    logger.error(f"Error closing {platform.platform_name}: {e}")