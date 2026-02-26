"""Twitch platform integration.

Handles OAuth token management with SQLite-backed storage,
stream title and category updates via the Helix API.
"""
import asyncio
import logging
import os
import re
import sqlite3
import time
import webbrowser
import requests
from typing import Optional
from urllib.parse import quote
from integrations.platforms.base.stream_platform import StreamPlatform
from config.constants import _PROJECT_ROOT

logger = logging.getLogger(__name__)

# Twitch OAuth endpoints
TWITCH_AUTH_URL = "https://id.twitch.tv/oauth2/authorize"
TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
TWITCH_SCOPES = "channel:manage:broadcast"


class TwitchTokenManager:
    """Manages Twitch OAuth user tokens in SQLite for persistent storage."""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            core_dir = os.path.join(_PROJECT_ROOT, "core")
            os.makedirs(core_dir, exist_ok=True)
            db_path = os.path.join(core_dir, "twitch_tokens.db")
        self.db_path = db_path
        self._ensure_table()

    def _ensure_table(self):
        """Create tokens table if it doesn't exist."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    broadcaster_id TEXT PRIMARY KEY,
                    access_token TEXT NOT NULL,
                    refresh_token TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def get_tokens(self, broadcaster_id: str) -> Optional[dict]:
        """Load stored tokens for a broadcaster."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute(
                "SELECT access_token, refresh_token FROM tokens WHERE broadcaster_id = ?",
                (broadcaster_id,)
            )
            row = cursor.fetchone()
            if row:
                return {"access_token": row[0], "refresh_token": row[1]}
            return None
        finally:
            conn.close()

    def save_tokens(self, broadcaster_id: str, access_token: str, refresh_token: str):
        """Store or update tokens for a broadcaster."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT INTO tokens (broadcaster_id, access_token, refresh_token, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(broadcaster_id) DO UPDATE SET
                    access_token = excluded.access_token,
                    refresh_token = excluded.refresh_token,
                    created_at = excluded.created_at
            """, (broadcaster_id, access_token, refresh_token, time.time()))
            conn.commit()
        finally:
            conn.close()

    def clear_tokens(self, broadcaster_id: str):
        """Remove stored tokens for a broadcaster."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("DELETE FROM tokens WHERE broadcaster_id = ?", (broadcaster_id,))
            conn.commit()
        finally:
            conn.close()


class TwitchUpdater(StreamPlatform):
    """Twitch platform integration with OAuth authorization code flow."""

    def __init__(self, client_id: str, client_secret: str, broadcaster_id: str,
                 redirect_uri: str = "http://localhost:8080/callback"):
        super().__init__("Twitch")
        self.client_id = client_id
        self.client_secret = client_secret
        self.broadcaster_id = broadcaster_id
        self.redirect_uri = redirect_uri
        self.base_url = "https://api.twitch.tv/helix"

        self.token_manager = TwitchTokenManager()
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self._initialized = False

    def _ensure_authenticated(self):
        """Ensure we have a valid user access token, prompting auth flow if needed."""
        if self._initialized and self.access_token:
            return

        # Try loading stored tokens
        stored = self.token_manager.get_tokens(self.broadcaster_id)
        if stored:
            self.access_token = stored["access_token"]
            self.refresh_token = stored["refresh_token"]
            self._initialized = True
            logger.info(f"[{self.platform_name}] Loaded cached user tokens")
            return

        # No tokens — run interactive OAuth authorization code flow
        logger.info(f"[{self.platform_name}] No user tokens found. Starting OAuth authorization flow...")
        self._run_auth_flow()

    def _run_auth_flow(self):
        """Run the interactive OAuth authorization code flow."""
        auth_url = (
            f"{TWITCH_AUTH_URL}"
            f"?response_type=code"
            f"&client_id={self.client_id}"
            f"&redirect_uri={self.redirect_uri}"
            f"&scope={quote(TWITCH_SCOPES)}"
        )

        logger.info(f"[{self.platform_name}] Opening authorization URL in browser...")
        webbrowser.open(auth_url)

        logger.info(f"[{self.platform_name}] Authorization URL: {auth_url}")
        logger.info(
            f"[{self.platform_name}] After authorizing, you'll be redirected to {self.redirect_uri}. "
            "The page won't load — that's expected. Copy the URL from the address bar."
        )

        user_input = input(
            f"[{self.platform_name}] Paste the redirect URL (or just the code): "
        ).strip()

        if not user_input:
            raise RuntimeError("OAuth authorization code is required.")

        # Extract code from full URL or use raw input
        code = self._extract_code(user_input)
        if not code:
            raise RuntimeError(f"Could not extract authorization code from: {user_input}")

        logger.info(f"[{self.platform_name}] Exchanging authorization code for tokens...")
        self._exchange_code(code)

    def _extract_code(self, user_input: str) -> Optional[str]:
        """Extract authorization code from a URL or raw code string."""
        # Try to extract from URL: http://localhost:3000/?code=XYZ&scope=...
        match = re.search(r'[?&]code=([^&\s]+)', user_input)
        if match:
            return match.group(1)

        # If no URL pattern, treat the whole input as the code
        if not user_input.startswith("http"):
            return user_input

        return None

    def _exchange_code(self, code: str):
        """Exchange authorization code for access + refresh tokens."""
        response = requests.post(TWITCH_TOKEN_URL, data={
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": self.redirect_uri,
        }, timeout=10)
        response.raise_for_status()

        data = response.json()
        access_token: str = data["access_token"]
        refresh_token: str = data["refresh_token"]
        self.access_token = access_token
        self.refresh_token = refresh_token
        self._initialized = True

        self.token_manager.save_tokens(self.broadcaster_id, access_token, refresh_token)
        logger.info(f"[{self.platform_name}] Tokens exchanged and saved successfully!")

    def _refresh_access_token(self) -> bool:
        """Refresh the access token using the stored refresh token.
        
        Returns True if refresh succeeded, False if re-auth is needed.
        """
        if not self.refresh_token:
            logger.error(f"[{self.platform_name}] No refresh token available")
            return False

        try:
            response = requests.post(TWITCH_TOKEN_URL, data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
                "refresh_token": quote(self.refresh_token, safe=""),
            }, timeout=10)

            if response.status_code == 401 or response.status_code == 400:
                error_data = response.json()
                logger.error(
                    f"[{self.platform_name}] Refresh token invalid: {error_data.get('message', 'unknown')}. "
                    "You will need to re-authorize on next startup."
                )
                self.token_manager.clear_tokens(self.broadcaster_id)
                self._initialized = False
                self.access_token = None
                self.refresh_token = None
                return False

            response.raise_for_status()
            data = response.json()

            access_token: str = data["access_token"]
            refresh_token: str = data["refresh_token"]
            self.access_token = access_token
            self.refresh_token = refresh_token
            self.token_manager.save_tokens(self.broadcaster_id, access_token, refresh_token)

            logger.info(f"[{self.platform_name}] Access token refreshed successfully")
            return True

        except requests.RequestException as e:
            logger.error(f"[{self.platform_name}] Failed to refresh token: {e}")
            return False

    def _get_headers(self) -> dict:
        """Get standard Twitch API headers."""
        return {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def _request_with_refresh(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make an API request, auto-refreshing the token on 401.
        
        Args:
            method: HTTP method (get, post, patch, etc.)
            url: Request URL
            **kwargs: Passed to requests.request()
            
        Returns:
            Response object
            
        Raises:
            requests.RequestException: If request fails after refresh attempt
        """
        self._ensure_authenticated()

        kwargs.setdefault("headers", self._get_headers())
        kwargs.setdefault("timeout", 10)

        response = requests.request(method, url, **kwargs)

        if response.status_code == 401:
            logger.info(f"[{self.platform_name}] Got 401, attempting token refresh...")
            if self._refresh_access_token():
                # Retry with new token
                kwargs["headers"] = self._get_headers()
                response = requests.request(method, url, **kwargs)
            else:
                response.raise_for_status()

        return response

    def update_token(self, new_token: str):
        """Update access token (called by platform manager with app token).
        
        We ignore this — TwitchUpdater manages its own user token.
        The app token from TwitchLiveChecker is only for live checks.
        """
        pass

    async def update_title(self, title: str) -> bool:
        """Update Twitch stream title."""
        url = f"{self.base_url}/channels"
        params = {"broadcaster_id": self.broadcaster_id}
        data = {"title": title}

        try:
            response = await asyncio.to_thread(
                self._request_with_refresh,
                "PATCH", url, params=params, json=data
            )
            response.raise_for_status()
            self.log_success("Updated title", title)
            return True
        except requests.RequestException as e:
            self.log_error("Update title", e)
            return False

    async def update_category(self, category_name: str) -> bool:
        """Update Twitch stream category/game."""
        game_id = await self._get_game_id(category_name)
        if not game_id:
            logger.warning(f"[{self.platform_name}] Could not find game ID for: {category_name}")
            return False

        url = f"{self.base_url}/channels"
        params = {"broadcaster_id": self.broadcaster_id}
        data = {"game_id": game_id}

        try:
            response = await asyncio.to_thread(
                self._request_with_refresh,
                "PATCH", url, params=params, json=data
            )
            response.raise_for_status()
            self.log_success("Updated category", category_name)
            return True
        except requests.RequestException as e:
            self.log_error("Update category", e)
            return False

    async def _get_game_id(self, game_name: str) -> Optional[str]:
        """Get Twitch game ID from game name.

        Runs the blocking ``requests`` call in a background thread
        so the event loop stays responsive.
        """
        url = f"{self.base_url}/games"
        params = {"name": game_name}

        try:
            response = await asyncio.to_thread(
                self._request_with_refresh, "GET", url, params=params
            )
            response.raise_for_status()
            data = response.json()

            if data.get("data"):
                return data["data"][0]["id"]
            return None
        except requests.RequestException as e:
            self.log_error("Get game ID", e)
            return None

    async def update_stream_info(self, title: str, category: Optional[str] = None) -> bool:
        """Update both title and category in one API call."""
        url = f"{self.base_url}/channels"
        params = {"broadcaster_id": self.broadcaster_id}
        data = {"title": title}

        if category:
            game_id = await self._get_game_id(category)
            if game_id:
                data["game_id"] = game_id

        try:
            response = await asyncio.to_thread(
                self._request_with_refresh,
                "PATCH", url, params=params, json=data
            )
            response.raise_for_status()
            self.log_success("Updated stream info", f"Title: {title}, Category: {category or 'N/A'}")
            return True
        except requests.RequestException as e:
            self.log_error("Update stream info", e)
            return False