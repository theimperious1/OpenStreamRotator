import requests
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class NotificationService:
    """Centralized service for sending notifications to various platforms."""

    def __init__(self, discord_webhook_url: Optional[str] = None):
        """
        Initialize notification service.
        
        Args:
            discord_webhook_url: Discord webhook URL for notifications
        """
        self.discord_webhook_url = discord_webhook_url

    def send_discord(self, title: str, description: str, color: int = 0x00FF00):
        """
        Send a Discord notification via webhook.
        
        Args:
            title: Embed title
            description: Embed description
            color: Hex color code (default green = 0x00FF00)
        """
        if not self.discord_webhook_url:
            logger.debug("Discord webhook not configured, skipping notification")
            return

        payload = {
            "embeds": [{
                "title": title,
                "description": description,
                "color": color,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }]
        }
        
        try:
            response = requests.post(self.discord_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logger.debug(f"Discord notification sent: {title}")
        except requests.RequestException as e:
            logger.error(f"Failed to send Discord notification: {e}")

    def notify_stream_update_failed(self, platform: str):
        """Notify that stream update failed on a platform."""
        self.send_discord(
            f"{platform} Title Update Failed",
            f"Failed to update title on {platform}",
            color=0xFF0000
        )

    def notify_stream_info_update_failed(self, platform: str):
        """Notify that stream info update failed on a platform."""
        self.send_discord(
            f"{platform} Stream Update Failed",
            f"Failed to update stream info on {platform}",
            color=0xFF0000
        )

    def notify_rotation_started(self, playlist_names: list[str]):
        """Notify that content rotation has started."""
        self.send_discord(
            "Content Rotation Started",
            f"Downloading: {', '.join(playlist_names)}",
            color=0xFFA500
        )

    def notify_rotation_error(self, message: str):
        """Notify about rotation errors."""
        self.send_discord(
            "Rotation Error",
            message,
            color=0xFF0000
        )

    def notify_download_warning(self, message: str):
        """Notify about download warnings."""
        self.send_discord(
            "Download Warning",
            message,
            color=0xFF0000
        )

    def notify_next_rotation_ready(self, playlist_names: list[str]):
        """Notify that next rotation is ready."""
        self.send_discord(
            "Next Rotation Ready",
            f"Downloaded: {', '.join(playlist_names)}",
            color=0x00FF00
        )

    def notify_background_download_warning(self):
        """Notify about background download warnings."""
        self.send_discord(
            "Background Download Warning",
            "Some playlists failed to download in background",
            color=0xFF9900
        )

    def notify_background_download_error(self, error_message: str):
        """Notify about background download errors."""
        self.send_discord(
            "Background Download Error",
            f"Failed to download next rotation: {error_message}",
            color=0xFF0000
        )

    def notify_playback_skip(self, time_skipped_seconds: float, new_finish_time_str: str):
        """Notify about detected playback skip."""
        self.send_discord(
            "Playback Skip Detected",
            f"Video position jumped {time_skipped_seconds:.1f}s ahead. "
            f"Rotation finish time recalculated to: {new_finish_time_str}",
            color=0x0099FF
        )

    def notify_asmongold_live(self):
        """Notify that Asmongold is live."""
        self.send_discord(
            "Asmongold is LIVE!",
            "24/7 stream paused",
            color=0x9146FF
        )

    def notify_asmongold_offline(self):
        """Notify that Asmongold is offline."""
        self.send_discord(
            "Asmongold is OFFLINE",
            "24/7 stream resumed",
            color=0x00FF00
        )

    def notify_automation_error(self, error_message: str):
        """Notify about general automation errors."""
        self.send_discord(
            "Automation Error",
            f"Unexpected error: {error_message}",
            color=0xFF0000
        )
    def notify_override_complete(self, resumed_playlist_names: list[str]):
        """Notify that manual override is complete and returning to original content."""
        self.send_discord(
            "Override Complete",
            f"Returning to: {', '.join(resumed_playlist_names)}",
            color=0x00FF00
        )