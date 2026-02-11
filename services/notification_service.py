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

    def notify_rotation_switched(self, playlist_names: list[str]):
        """Notify that a rotation content switch completed successfully."""
        self.send_discord(
            "Now Playing",
            f"Switched to: **{', '.join(playlist_names)}**",
            color=0x00FF00
        )

    def notify_temp_playback_activated(self, file_count: int):
        """Notify that temp playback mode was activated."""
        self.send_discord(
            "Temp Playback Activated",
            f"Long download detected — streaming {file_count} ready files while download continues",
            color=0xFFA500
        )

    def notify_temp_playback_exited(self, playlist_names: list[str]):
        """Notify that temp playback mode exited and normal rotation resumed."""
        self.send_discord(
            "Temp Playback Complete",
            f"Download finished, switched to: **{', '.join(playlist_names)}**",
            color=0x00FF00
        )

    def notify_session_resumed(self, session_id: int, video: Optional[str] = None, cursor_s: Optional[float] = None):
        """Notify that a session was resumed (crash recovery)."""
        desc = f"Resumed session **#{session_id}**"
        if video and cursor_s is not None and cursor_s > 0:
            minutes, seconds = divmod(int(cursor_s), 60)
            desc += f"\nResuming **{video}** at {minutes}:{seconds:02d}"
        self.send_discord(
            "Session Resumed",
            desc,
            color=0x0099FF
        )

    def notify_video_transition(self, video_name: str, category: Optional[str] = None):
        """Notify about a video transition (optional, can be noisy)."""
        desc = f"**{video_name}**"
        if category:
            desc += f" ({category})"
        self.send_discord(
            "Video Transition",
            desc,
            color=0x808080
        )

    def notify_automation_started(self):
        """Notify that the automation system has started."""
        self.send_discord(
            "Automation Started",
            "24/7 stream automation is online",
            color=0x00FF00
        )

    def notify_automation_shutdown(self):
        """Notify that the automation system is shutting down."""
        self.send_discord(
            "Automation Shutting Down",
            "24/7 stream automation is going offline",
            color=0xFF9900
        )

    def notify_streamer_live(self):
        """Notify that the streamer is live."""
        self.send_discord(
            "Streamer is LIVE!",
            "24/7 stream paused",
            color=0x9146FF
        )

    def notify_streamer_offline(self):
        """Notify that the streamer is offline."""
        self.send_discord(
            "Streamer is OFFLINE",
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
