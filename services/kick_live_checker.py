"""Kick live status checker.

Acquires Kick app access tokens via client credentials and
checks whether specified channels are currently streaming,
using the official Kick public API.
"""
import requests
import time
import logging
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

KICK_TOKEN_URL = "https://id.kick.com/oauth/token"
KICK_CHANNELS_URL = "https://api.kick.com/public/v1/channels"


class KickLiveChecker:

    def __init__(self, client_id: str, client_secret: str):
        """
        Initialize Kick live checker.

        Args:
            client_id: Kick application client ID
            client_secret: Kick application client secret
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token: Optional[str] = None
        self.token_expiry: float = 0

    def get_app_access_token(self) -> Tuple[str, float]:
        """
        Get Kick App Access Token via client credentials grant.

        Returns:
            Tuple of (token, expiry_timestamp)

        Raises:
            requests.RequestException: If token acquisition fails
        """
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
        try:
            response = requests.post(KICK_TOKEN_URL, data=data, timeout=10)
            response.raise_for_status()
            body = response.json()
            token = body["access_token"]
            expiry = time.time() + body.get("expires_in", 3600)
            logger.info("Kick app access token acquired")
            return token, expiry
        except requests.RequestException as e:
            logger.error(f"Failed to get Kick token: {e}")
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
            logger.error(f"Failed to refresh Kick token: {e}")
            return False

    def is_stream_live(self, channel_slug: str) -> bool:
        """
        Check if a Kick channel is live via the public channels API.

        Args:
            channel_slug: Kick channel slug (e.g. 'xqc')

        Returns:
            True if channel is live, False otherwise
        """
        if not self.token:
            logger.debug("No Kick token available for live check")
            return False

        headers = {"Authorization": f"Bearer {self.token}"}
        params = {"slug": [channel_slug]}

        try:
            response = requests.get(
                KICK_CHANNELS_URL, headers=headers, params=params, timeout=10
            )
            response.raise_for_status()
            body = response.json()
            channels = body.get("data", [])
            if channels:
                stream = channels[0].get("stream") or {}
                is_live = stream.get("is_live", False)
                logger.debug(f"Checked Kick {channel_slug} live status: {is_live}")
                return is_live
            logger.debug(f"Kick channel '{channel_slug}' not found in API response")
            return False
        except requests.RequestException as e:
            logger.error(f"Failed to check Kick stream status for {channel_slug}: {e}")
            return False
