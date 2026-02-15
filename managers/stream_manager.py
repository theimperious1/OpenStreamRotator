"""Stream metadata manager.

Thin facade that delegates stream title and category updates
to the PlatformManager for all configured platforms.
"""
import logging
from typing import Optional
from managers.platform_manager import PlatformManager

logger = logging.getLogger(__name__)


class StreamManager:

    def __init__(self, platform_manager: PlatformManager):
        """
        Initialize stream manager.
        
        Args:
            platform_manager: PlatformManager instance for platform access
        """
        self.platform_manager = platform_manager

    async def update_title(self, title: str) -> bool:
        """
        Update stream title on all enabled platforms.
        
        Args:
            title: New stream title
            
        Returns:
            True if at least one platform succeeded
        """
        if not title:
            return False
        
        results = await self.platform_manager.update_title_all(title)
        if not results:
            logger.debug("No platforms configured for title update")
            return True
        
        success_count = sum(1 for success in results.values() if success)
        if success_count > 0:
            logger.info(f"Updated title on {success_count}/{len(results)} platforms: {title}")
        return success_count > 0

    async def update_category(self, category: 'dict[str, str] | str') -> bool:
        """
        Update stream category on all enabled platforms.
        
        Args:
            category: Per-platform dict ``{"twitch": ..., "kick": ...}``
                      or a single string for all platforms.
            
        Returns:
            True if at least one platform succeeded
        """
        if not category:
            logger.debug("No category provided for update")
            return True
        
        results = await self.platform_manager.update_category_all(category)
        if not results:
            logger.debug("No platforms configured for category update")
            return True
        
        success_count = sum(1 for success in results.values() if success)
        if success_count > 0:
            logger.info(f"Updated category on {success_count}/{len(results)} platforms: {category}")
        return success_count > 0

    async def update_both(self, title: str, category: 'dict[str, str] | str | None' = None) -> bool:
        """
        Update both title and category on all platforms (legacy method).
        
        Args:
            title: New stream title
            category: Per-platform dict or single string (optional)
            
        Returns:
            True if successful
        """
        title_updated = await self.update_title(title)
        category_updated = True
        
        if category:
            category_updated = await self.update_category(category)
        
        return title_updated or category_updated

    async def update_stream_info(self, title: str, category: 'dict[str, str] | str | None' = None) -> bool:
        """
        Update stream information (title and category together).
        Uses platform's update_stream_info method for optimal compatibility.
        
        Args:
            title: New stream title
            category: Per-platform dict or single string (optional)
            
        Returns:
            True if successful
        """
        if not title:
            return False
        
        results = await self.platform_manager.update_stream_info_all(title, category)
        if not results:
            logger.debug("No platforms configured for stream info update")
            return True
        
        success_count = sum(1 for success in results.values() if success)
        if success_count > 0:
            logger.info(f"Updated stream info on {success_count}/{len(results)} platforms: title='{title}', category='{category}'")
        return success_count > 0
