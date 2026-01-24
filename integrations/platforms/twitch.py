import requests
import logging
from typing import Optional
from integrations.platforms.base.stream_platform import StreamPlatform

logger = logging.getLogger(__name__)

class TwitchUpdater(StreamPlatform):
    """Twitch platform integration."""

    def __init__(self, client_id: str, access_token: str, broadcaster_id: str):
        super().__init__("Twitch")
        self.client_id = client_id
        self.access_token = access_token
        self.broadcaster_id = broadcaster_id
        self.base_url = "https://api.twitch.tv/helix"

    def _get_headers(self) -> dict:
        """Get standard Twitch API headers."""
        return {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def update_token(self, new_token: str):
        """Update access token when it's refreshed."""
        self.access_token = new_token

    async def update_title(self, title: str) -> bool:
        """Update Twitch stream title."""
        url = f"{self.base_url}/channels"
        params = {"broadcaster_id": self.broadcaster_id}
        data = {"title": title}

        try:
            response = requests.patch(
                url,
                headers=self._get_headers(),
                params=params,
                json=data,
                timeout=10
            )
            response.raise_for_status()
            self.log_success("Updated title", title)
            return True
        except requests.RequestException as e:
            self.log_error("Update title", e)
            return False

    def update_category(self, category_name: str) -> bool:
        """Update Twitch stream category/game."""
        # First, get the game ID
        game_id = self._get_game_id(category_name)
        if not game_id:
            logger.warning(f"[{self.platform_name}] Could not find game ID for: {category_name}")
            return False

        url = f"{self.base_url}/channels"
        params = {"broadcaster_id": self.broadcaster_id}
        data = {"game_id": game_id}

        try:
            response = requests.patch(
                url,
                headers=self._get_headers(),
                params=params,
                json=data,
                timeout=10
            )
            response.raise_for_status()
            self.log_success("Updated category", category_name)
            return True
        except requests.RequestException as e:
            self.log_error("Update category", e)
            return False

    async def _get_game_id(self, game_name: str) -> Optional[str]:
        """Get Twitch game ID from game name."""
        url = f"{self.base_url}/games"
        params = {"name": game_name}

        try:
            response = requests.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=10
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
        """
        Update both title and category in one API call (more efficient).
        """
        url = f"{self.base_url}/channels"
        params = {"broadcaster_id": self.broadcaster_id}
        data = {"title": title}

        # Add game_id if category specified
        if category:
            game_id = await self._get_game_id(category)
            if game_id:
                data["game_id"] = game_id

        try:
            response = requests.patch(
                url,
                headers=self._get_headers(),
                params=params,
                json=data,
                timeout=10
            )
            response.raise_for_status()
            self.log_success("Updated stream info", f"Title: {title}, Category: {category or 'N/A'}")
            return True
        except requests.RequestException as e:
            self.log_error("Update stream info", e)
            return False