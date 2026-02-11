import logging
import os
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

    def setup(self, twitch_live_checker=None) -> None:
        """Initialize enabled streaming platforms from environment configuration.
        
        Args:
            twitch_live_checker: Optional TwitchLiveChecker instance for token management
        """
        # Load configuration from environment
        enable_twitch = os.getenv("ENABLE_TWITCH", "false").lower() == "true"
        enable_kick = os.getenv("ENABLE_KICK", "false").lower() == "true"
        
        twitch_client_id = os.getenv("TWITCH_CLIENT_ID", "")
        twitch_client_secret = os.getenv("TWITCH_CLIENT_SECRET", "")
        twitch_user_login = os.getenv("TWITCH_USER_LOGIN", "")
        twitch_broadcaster_id = os.getenv("TWITCH_BROADCASTER_ID", "")
        twitch_redirect_uri = os.getenv("TWITCH_REDIRECT_URI", "http://localhost:8080/callback")
        
        kick_client_id = os.getenv("KICK_CLIENT_ID", "")
        kick_client_secret = os.getenv("KICK_CLIENT_SECRET", "")
        kick_channel_id = os.getenv("KICK_CHANNEL_ID", "")
        kick_redirect_uri = os.getenv("KICK_REDIRECT_URI", "http://localhost:8080/callback")
        
        # Setup Twitch live checker first
        if twitch_live_checker:
            try:
                twitch_live_checker.refresh_token_if_needed()
                logger.info("Twitch credentials available for live status checking")
            except Exception as e:
                logger.warning(f"Could not initialize Twitch live checker: {e}")
        
        # Setup Twitch platform
        if enable_twitch and twitch_client_id and twitch_client_secret and twitch_live_checker:
            try:
                broadcaster_id = twitch_broadcaster_id
                if not broadcaster_id and twitch_user_login:
                    broadcaster_id = twitch_live_checker.get_broadcaster_id(twitch_user_login)

                if broadcaster_id:
                    self.add_twitch(
                        twitch_client_id,
                        twitch_client_secret,
                        broadcaster_id,
                        twitch_redirect_uri
                    )
                    logger.info(f"Twitch enabled for channel: {twitch_user_login}")
                else:
                    logger.warning("Twitch broadcaster ID not found")
            except Exception as e:
                logger.error(f"Failed to setup Twitch: {e}")

        # Setup Kick platform
        if enable_kick and kick_client_id and kick_client_secret and kick_channel_id:
            self.add_kick(
                kick_client_id, kick_client_secret, kick_channel_id, kick_redirect_uri
            )
            logger.info(f"Kick enabled for channel ID: {kick_channel_id}")

        # Log summary
        enabled = self.get_enabled_platforms()
        if enabled:
            logger.info(f"Enabled platforms: {', '.join(enabled)}")
        else:
            logger.warning("No streaming platforms enabled")

    def add_twitch(self, client_id: str, client_secret: str, broadcaster_id: str,
                    redirect_uri: str = "http://localhost:8080/callback"):
        """Add Twitch platform integration."""
        try:
            twitch = TwitchUpdater(client_id, client_secret, broadcaster_id, redirect_uri)
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

    async def update_category_all(self, category: str) -> dict[str, bool]:
        """Update category on all enabled platforms."""
        results = {}
        for platform in self.platforms:
            results[platform.platform_name] = await platform.update_category(category)
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
            try:
                if hasattr(platform, 'close'):
                    platform.close()  # type: ignore
            except Exception:
                pass