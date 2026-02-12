"""Twitch live status checker.

Acquires Twitch app access tokens via client credentials and
checks whether specified channels are currently streaming.
"""
import requests
import time
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


class TwitchLiveChecker:

    def __init__(self, client_id: str, client_secret: str):
        """
        Initialize Twitch live checker.
        
        Args:
            client_id: Twitch application client ID
            client_secret: Twitch application client secret
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_expiry = 0

    def get_app_access_token(self) -> Tuple[str, float]:
        """
        Get Twitch App Access Token.
        
        Returns:
            Tuple of (token, expiry_timestamp)
        
        Raises:
            requests.RequestException: If token acquisition fails
        """
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials"
        }
        try:
            response = requests.post(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            token = data["access_token"]
            expiry = time.time() + data.get("expires_in", 3600)
            logger.info("Twitch app access token acquired")
            return token, expiry
        except requests.RequestException as e:
            logger.error(f"Failed to get Twitch token: {e}")
            raise

    def refresh_token_if_needed(self) -> bool:
        """
        Refresh token if it has expired.
        
        Returns:
            True if token was refreshed or already valid
        """
        if self.token and time.time() < self.token_expiry:
            return True
        
        try:
            self.token, self.token_expiry = self.get_app_access_token()
            return True
        except Exception as e:
            logger.error(f"Failed to refresh Twitch token: {e}")
            return False

    def get_broadcaster_id(self, username: str) -> str:
        """
        Get broadcaster ID from username.
        
        Args:
            username: Twitch username
        
        Returns:
            Broadcaster ID or empty string if not found
        """
        if not self.token:
            logger.error("No Twitch token available")
            return ""
        
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.token}"
        }
        url = f"https://api.twitch.tv/helix/users?login={username}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("data"):
                broadcaster_id = data["data"][0]["id"]
                logger.info(f"Got broadcaster ID for {username}: {broadcaster_id}")
                return broadcaster_id
            return ""
        except requests.RequestException as e:
            logger.error(f"Failed to get broadcaster ID: {e}")
            return ""

    def is_stream_live(self, username: str) -> bool:
        """
        Check if a Twitch user is live.
        
        Args:
            username: Twitch username to check
        
        Returns:
            True if user is live, False otherwise
        """
        if not self.token:
            logger.debug("No Twitch token available for live check")
            return False
        
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.token}"
        }
        url = f"https://api.twitch.tv/helix/streams?user_login={username}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            is_live = bool(data.get("data"))
            logger.debug(f"Checked {username} live status: {is_live}")
            return is_live
        except requests.RequestException as e:
            logger.error(f"Failed to check stream status for {username}: {e}")
            return False
