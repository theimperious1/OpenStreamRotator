import asyncio
import logging
import os
import re
import sqlite3
from typing import Optional
import aiohttp
from integrations.platforms.base.stream_platform import StreamPlatform
from config.constants import KICK_FALLBACK_CATEGORY_ID

logger = logging.getLogger(__name__)

# Import from local embedded kickpython library
from lib.kickpython.kickpython.api import KickAPI
KICK_AVAILABLE = True

class KickUpdater(StreamPlatform):
    """Kick platform integration using kickpython library."""

    def __init__(self, client_id: str, client_secret: str, channel_id: str,
                 redirect_uri: str = "http://localhost:8080/callback",
                 scopes: Optional[list[str]] = None,
                 db_path: Optional[str] = None):
        super().__init__("Kick")

        self.client_id = client_id
        self.client_secret = client_secret
        self.channel_id = channel_id
        self.redirect_uri = redirect_uri
        self.scopes = scopes or ["channel:read", "channel:write"]
        
        # Use core directory for token storage if not provided
        if db_path is None:
            core_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "core")
            db_path = os.path.join(core_dir, "kick_tokens.db")
        
        self.db_path = db_path

        self.api: Optional[KickAPI] = None          # Authenticated client
        self.public_api: Optional[KickAPI] = None   # Public client (no auth)
        self.loop = None
        self._initialized = False

    async def _ensure_initialized(self):
        """Initialize both API clients and verify authentication."""
        if self._initialized:
            return

        try:
            # Get or create event loop
            try:
                self.loop = asyncio.get_running_loop()
            except RuntimeError:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)

            # Authenticated client (for updates)
            self.api = KickAPI(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                db_path=self.db_path,
            )

            # Public client (for categories, no auth header)
            self.public_api = KickAPI(
                client_id=self.client_id,  # still needed internally
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                db_path=self.db_path,
            )

            # Check if tokens exist without calling asyncio.run()
            # This avoids the "asyncio.run() cannot be called from a running event loop" error
            db_path = self.db_path
            tokens_exist = False
            
            try:
                if os.path.exists(db_path):
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM tokens WHERE channel_id = ?", (self.channel_id,))
                    count = cursor.fetchone()[0]
                    tokens_exist = count > 0
                    conn.close()
            except Exception as db_err:
                logger.debug(f"[{self.platform_name}] Error checking tokens DB: {db_err}")

            if not tokens_exist:
                logger.info(
                    f"[{self.platform_name}] No valid tokens found. "
                    "You need to complete the OAuth flow once."
                )
                auth_data = self.api.get_auth_url(self.scopes)
                auth_url = auth_data["auth_url"]
                code_verifier = auth_data["code_verifier"]

                logger.info(f"[{self.platform_name}] Please visit:\n{auth_url}")
                logger.info(f"[{self.platform_name}] After authorizing → copy 'code' from redirect URL.")
                logger.info(f"[{self.platform_name}] Edit authorize_kick.py and set \'code\' to the code you copied.")
                logger.info(f"[{self.platform_name}] Then set code_verifier to ${code_verifier}")
                logger.info(f"[{self.platform_name}] Finally: Run \'python authorize_kick.py\'. Once done, restart main.py.")

                raise RuntimeError("OAuth authorization required.")
            else:
                logger.info(f"[{self.platform_name}] Found cached tokens, using stored credentials")

            logger.info(f"[{self.platform_name}] API initialized successfully")
            self._initialized = True

        except Exception as e:
            self.log_error("Initialization failed", e)
            raise

    async def _update_channel(self, **kwargs):
        """Internal async method to update channel. Category ID is required by the API."""
        await self._ensure_initialized()

        # category_id is required by KickAPI.update_channel()
        category_id = kwargs.pop("category_id", None)
        if category_id is None:
            # Skip trying to fetch current category to avoid kickpython's asyncio.run() issue
            # Always use the provided category or fallback
            category_id = KICK_FALLBACK_CATEGORY_ID  # Just Chatting - fallback
            logger.warning(f"[{self.platform_name}] Using fallback category ID: {category_id}")

        # Ensure category_id is an integer
        try:
            category_id = int(category_id)
        except (ValueError, TypeError):
            logger.error(f"[{self.platform_name}] Invalid category ID: {category_id}, using fallback")
            category_id = KICK_FALLBACK_CATEGORY_ID

        update_params = {
            "channel_id": self.channel_id,
            "category_id": category_id,
            **kwargs
        }
        
        logger.debug(f"[{self.platform_name}] Calling update_channel with: channel_id={self.channel_id}, category_id={category_id}, {kwargs}")
        await self.api.update_channel(**update_params)  # type: ignore

    async def update_title(self, title: str) -> bool:
        try:
            await self._update_channel(stream_title=title)  # No category_id needed — it will fetch current
            self.log_success("Updated title", title)
            return True
        except Exception as e:
            error_str = str(e)
            # 204 No Content is actually a success - the API successfully updated but returned no body
            if "204" in error_str and "ContentTypeError" in type(e).__name__:
                self.log_success("Updated title", title)
                logger.info(f"[{self.platform_name}] Update successful (API returned 204 No Content)")
                return True
            logger.error(f"[{self.platform_name}] Update title failed with error: {type(e).__name__}: {e}", exc_info=True)
            self.log_error("Update title", e)
            return False

    async def _get_category_id(self, category_name: str) -> Optional[str]:
        """Get subcategory ID from Kick API."""
        try:
            logger.info(f"[{self.platform_name}] Searching for subcategory: {category_name}")
            
            # Use Kick private API endpoint to search for subcategory
            url = f"https://api.kick.com/private/v1/categories/{category_name.lower()}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        logger.error(f"[{self.platform_name}] Failed to fetch categories: HTTP {response.status}")
                        return None
                    
                    data = await response.json()
                    logger.debug(f"[{self.platform_name}] Raw API response for '{category_name}': {data}")
                    
                    # Response structure can vary:
                    # 1. Direct list: [{...}, {...}]
                    # 2. Dict with data key: {"data": [{...}]} or {"data": {...}}
                    # 3. Dict without data key: {...}
                    
                    if isinstance(data, list):
                        # Direct list response
                        if len(data) > 0:
                            category_data = data[0]
                        else:
                            logger.warning(f"[{self.platform_name}] Category not found on Kick: '{category_name}' (empty list)")
                            return None
                    elif isinstance(data, dict):
                        # Dict response - try to get "data" key first
                        data_obj = data.get("data")
                        
                        if data_obj is None:
                            logger.warning(f"[{self.platform_name}] No data in category response for '{category_name}'")
                            return None
                        
                        if not isinstance(data_obj, (dict, list)):
                            logger.warning(f"[{self.platform_name}] Unexpected data type in category response: {type(data_obj).__name__}")
                            return None
                        
                        # Handle if data_obj is a list (take first item)
                        if isinstance(data_obj, list):
                            if len(data_obj) > 0:
                                category_data = data_obj[0]
                            else:
                                logger.warning(f"[{self.platform_name}] Category not found on Kick: '{category_name}' (empty list in data)")
                                return None
                        else:
                            # data_obj is a dict
                            if "category" in data_obj:
                                category_data = data_obj.get("category")
                            else:
                                category_data = data_obj
                    else:
                        logger.warning(f"[{self.platform_name}] Unexpected response format for '{category_name}': {type(data).__name__}")
                        return None
                    
                    if category_data is None or not isinstance(category_data, dict):
                        logger.warning(f"[{self.platform_name}] Category data is not a dict for '{category_name}': got {type(category_data).__name__}")
                        return None
                    
                    # Extract the numeric subcategory ID from the image_url
                    # Format: https://files.kick.com/images/subcategories/11997/banner/...
                    image_url = category_data.get("image_url", "")
                    if image_url:
                        # Parse out the numeric ID
                        match = re.search(r'/subcategories/(\d+)/', image_url)
                        if match:
                            subcategory_id = match.group(1)
                            cat_name = category_data.get("name", "")
                            logger.info(f"[{self.platform_name}] Found subcategory: {cat_name} -> {subcategory_id}")
                            return subcategory_id
                    
                    logger.warning(f"[{self.platform_name}] No subcategory ID found for '{category_name}'")
                    return None
                    
        except asyncio.TimeoutError:
            logger.error(f"[{self.platform_name}] Timeout fetching categories from Kick API")
            return None
        except Exception as e:
            logger.error(f"[{self.platform_name}] Category lookup error for '{category_name}': {type(e).__name__}: {e}", exc_info=True)
            return None

    def update_category(self, category_name: str) -> bool:
        """Note: This is a sync wrapper. Use update_stream_info for category + title updates."""
        try:
            # For now, return False since we can't do async lookups in a sync context
            # The category lookup happens in update_stream_info instead
            logger.warning(f"[{self.platform_name}] Direct category update not supported, use update_stream_info")
            return False
        except Exception as e:
            self.log_error("Update category", e)
            return False

    async def update_stream_info(self, title: str, category: Optional[str] = None) -> bool:
        try:
            params = {"stream_title": title}

            if category:
                category_id = await self._get_category_id(category)
                if category_id:
                    params["category_id"] = category_id  # This will override the current one

            await self._update_channel(**params)
            self.log_success(
                "Updated stream info",
                f"Title: {title}, Category: {category or 'N/A'}"
            )
            return True
        except Exception as e:
            error_str = str(e)
            # 204 No Content is actually a success - the API successfully updated but returned no body
            if "204" in error_str and "ContentTypeError" in type(e).__name__:
                self.log_success(
                    "Updated stream info",
                    f"Title: {title}, Category: {category or 'N/A'}"
                )
                logger.info(f"[{self.platform_name}] Update successful (API returned 204 No Content)")
                return True
            logger.error(f"[{self.platform_name}] Update stream info failed with error: {type(e).__name__}: {e}", exc_info=True)
            self.log_error("Update stream info", e)
            return False

    def close(self):
        if self.loop and not self.loop.is_closed():
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.run_until_complete(self.loop.shutdown_default_executor())
            self.loop.close()