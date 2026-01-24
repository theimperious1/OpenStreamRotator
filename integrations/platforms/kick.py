import asyncio
import json
import logging
import os
from typing import Optional
from integrations.platforms.base.stream_platform import StreamPlatform

logger = logging.getLogger(__name__)

from kickpython import KickAPI
KICK_AVAILABLE = True

class KickUpdater(StreamPlatform):
    """Kick platform integration using kickpython library."""

    def __init__(self, client_id: str, client_secret: str, channel_id: str,
                 redirect_uri: str = "http://localhost:8080/callback",
                 scopes: list[str] = None,
                 db_path: str = None):
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

            # Start token refresh for authenticated client
            asyncio.create_task(self.api.start_token_refresh())

            # Test authentication with a call that REQUIRES auth
            # (get_categories is public → won't trigger auth error properly)
            # Instead, use get_channel (requires channel:read scope)
            try:
                await self.api.get_users(channel_id=self.channel_id)
                logger.info(f"[{self.platform_name}] Already authenticated (tokens valid) - channel info loaded")
            except Exception as auth_exc:
                error_str = str(auth_exc).lower()
                if "unauthorized" in error_str or "token" in error_str or "auth" in error_str:
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
                    raise

            logger.info(f"[{self.platform_name}] API initialized successfully")
            self._initialized = True

        except Exception as e:
            self.log_error("Initialization failed", e)
            raise

    def _run_async(self, coro):
        """Helper to run async code from sync context."""
        if not self.loop:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        if self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            return future.result(timeout=30)
        else:
            return self.loop.run_until_complete(coro)

    async def _update_channel(self, **kwargs):
        """Internal async method to update channel. Always pass category_id."""
        await self._ensure_initialized()

        # Get current category ID if not provided
        category_id = kwargs.pop("category_id", None)
        if category_id is None:
            category_id = await self._get_current_category_id()
            if category_id is None:
                # Fallback: use a default valid category ID (e.g. "Just Chatting" ID is usually 1)
                category_id = "1"  # Change to actual ID if known
                logger.warning(f"[{self.platform_name}] Using fallback category ID: {category_id}")

        # Always include category_id
        await self.api.update_channel(
            channel_id=self.channel_id,
            category_id=category_id,
            **kwargs
        )

    async def update_title(self, title: str) -> bool:
        try:
            await self._update_channel(stream_title=title)  # No category_id needed — it will fetch current
            self.log_success("Updated title", title)
            return True
        except Exception as e:
            self.log_error("Update title", e)
            return False

    async def _get_category_id(self, category_name: str) -> Optional[str]:
        """Get category ID using the PUBLIC client (no auth required)."""
        await self._ensure_initialized()

        if self.public_api is None:
            raise RuntimeError("Public API not initialized")

        try:
            categories = await self.public_api.get_categories(query=category_name)
            if not categories:
                categories = await self.public_api.get_categories()  # full list

            logger.debug(f"Categories fetched: {json.dumps(categories, indent=2)}")

            for cat in categories:
                if isinstance(cat, dict) and cat.get("name", "").lower() == category_name.lower():
                    return cat.get("id")

            logger.warning(f"[{self.platform_name}] Category not found: {category_name}")
            return None
        except Exception as e:
            self.log_error("Get category ID", e)
            return None

    async def _get_current_category_id(self) -> Optional[str]:
        """Fetch the current category ID for the channel (requires auth)."""
        await self._ensure_initialized()
        try:
            # get_users() returns user/channel info including current category
            user_data = await self.api.get_users(channel_id=self.channel_id)
            # Depending on library response structure — adjust key as needed
            current_category = user_data.get("category_id") or user_data.get("channel", {}).get("category_id")
            if current_category:
                return str(current_category)
            logger.warning(f"[{self.platform_name}] Could not determine current category ID")
            return None
        except Exception as e:
            self.log_error("Get current category ID", e)
            return None

    def update_category(self, category_name: str) -> bool:
        try:
            category_id = self._run_async(self._get_category_id(category_name))
            if not category_id:
                return False

            self._run_async(self._update_channel(category_id=category_id))
            self.log_success("Updated category", category_name)
            return True
        except Exception as e:
            self.log_error("Update category", e)
            return False

    def update_stream_info(self, title: str, category: Optional[str] = None) -> bool:
        try:
            params = {"stream_title": title}

            if category:
                category_id = self._run_async(self._get_category_id(category))
                if category_id:
                    params["category_id"] = category_id  # This will override the current one

            self._run_async(self._update_channel(**params))
            self.log_success(
                "Updated stream info",
                f"Title: {title}, Category: {category or 'N/A'}"
            )
            return True
        except Exception as e:
            self.log_error("Update stream info", e)
            return False

    def close(self):
        if self.loop and not self.loop.is_closed():
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.run_until_complete(self.loop.shutdown_default_executor())
            self.loop.close()