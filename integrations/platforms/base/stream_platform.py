import logging
"""Abstract base class for streaming platform integrations.

Defines the common interface for title and category updates
that all platform implementations must provide.
"""
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


class StreamPlatform(ABC):

    def __init__(self, platform_name: str):
        self.platform_name = platform_name

    @abstractmethod
    async def update_title(self, title: str) -> bool:
        """Update stream title on the platform."""
        pass

    @abstractmethod
    async def update_category(self, category_name: str) -> bool:
        """Update stream category/game on the platform."""
        pass

    def update_token(self, new_token: str) -> None:
        """Update access token when refreshed. Override in subclasses if needed."""
        pass

    async def update_stream_info(self, title: str, category: Optional[str] = None) -> bool:
        """
        Update both title and category in one call.
        Platforms can override this for more efficient API usage.
        """
        success = await self.update_title(title)

        if category:
            category_success = await self.update_category(category)
            success = success and category_success

        return success

    def log_success(self, action: str, details: str = ""):
        """Log successful platform action."""
        msg = f"[{self.platform_name}] {action}"
        if details:
            msg += f": {details}"
        logger.info(msg)

    def log_error(self, action: str, error: Exception):
        """Log platform error."""
        logger.error(f"[{self.platform_name}] {action} failed: {error}")