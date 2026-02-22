"""Discord webhook notification service.

Sends rich embed notifications for rotation events, errors,
and status changes with local rate-limit tracking.
"""
import requests
import time
import logging
from typing import Optional
from config.constants import (
    COLOR_SUCCESS, COLOR_ERROR, COLOR_WARNING, COLOR_INFO,
    COLOR_STREAM_LIVE, COLOR_ROTATION_START, COLOR_NEXT_READY,
    COLOR_MUTED, COLOR_FALLBACK,
)

logger = logging.getLogger(__name__)

# Discord webhooks: 30 messages per 60 seconds per webhook
_DISCORD_RATE_LIMIT_WINDOW = 60.0
_DISCORD_RATE_LIMIT_MAX = 30


class NotificationService:

    def __init__(self, discord_webhook_url: Optional[str] = None):
        """
        Initialize notification service.
        
        Args:
            discord_webhook_url: Discord webhook URL for notifications
        """
        self.discord_webhook_url = discord_webhook_url
        self._discord_send_times: list[float] = []

    def send_discord(self, title: str, description: str, color: int = COLOR_SUCCESS):
        """
        Send a Discord notification via webhook with rate-limit awareness.
        
        Args:
            title: Embed title
            description: Embed description
            color: Hex color code (default green)
        """
        if not self.discord_webhook_url:
            logger.debug("Discord webhook not configured, skipping notification")
            return

        # Pre-flight rate limit check (local)
        now = time.time()
        self._discord_send_times = [t for t in self._discord_send_times if now - t < _DISCORD_RATE_LIMIT_WINDOW]
        if len(self._discord_send_times) >= _DISCORD_RATE_LIMIT_MAX:
            logger.warning(f"Discord rate limit reached ({_DISCORD_RATE_LIMIT_MAX}/{_DISCORD_RATE_LIMIT_WINDOW}s), dropping notification: {title}")
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
            self._discord_send_times.append(time.time())
            if response.status_code == 429:
                retry_after = response.json().get('retry_after', 1.0)
                logger.warning(f"Discord 429 rate limited, retry_after={retry_after}s — dropping: {title}")
                return
            response.raise_for_status()
            logger.debug(f"Discord notification sent: {title}")
        except requests.RequestException as e:
            logger.error(f"Failed to send Discord notification: {e}")

    def notify_stream_update_failed(self, platform: str):
        """Notify that stream update failed on a platform."""
        self.send_discord(
            f"{platform} Title Update Failed",
            f"Failed to update title on {platform}",
            color=COLOR_ERROR
        )

    def notify_stream_info_update_failed(self, platform: str):
        """Notify that stream info update failed on a platform."""
        self.send_discord(
            f"{platform} Stream Update Failed",
            f"Failed to update stream info on {platform}",
            color=COLOR_ERROR
        )

    def notify_rotation_started(self, playlist_names: list[str]):
        """Notify that content rotation has started."""
        self.send_discord(
            "Content Rotation Started",
            f"Downloading: {', '.join(playlist_names)}",
            color=COLOR_ROTATION_START
        )

    def notify_rotation_error(self, message: str):
        """Notify about rotation errors."""
        self.send_discord(
            "Rotation Error",
            message,
            color=COLOR_ERROR
        )

    def notify_download_warning(self, message: str):
        """Notify about download warnings."""
        self.send_discord(
            "Download Warning",
            message,
            color=COLOR_WARNING
        )

    def notify_next_rotation_ready(self, playlist_names: list[str]):
        """Notify that next rotation is ready."""
        self.send_discord(
            "Next Rotation Ready",
            f"Downloaded: {', '.join(playlist_names)}",
            color=COLOR_NEXT_READY
        )

    def notify_background_download_warning(self):
        """Notify about background download warnings."""
        self.send_discord(
            "Background Download Warning",
            "Some playlists failed to download in background",
            color=COLOR_WARNING
        )

    def notify_background_download_error(self, error_message: str):
        """Notify about background download errors."""
        self.send_discord(
            "Background Download Error",
            f"Failed to download next rotation: {error_message}",
            color=COLOR_ERROR
        )

    def notify_rotation_switched(self, playlist_names: list[str]):
        """Notify that a rotation content switch completed successfully."""
        self.send_discord(
            "Now Playing",
            f"Switched to: **{', '.join(playlist_names)}**",
            color=COLOR_SUCCESS
        )

    def notify_temp_playback_activated(self, file_count: int):
        """Notify that temp playback mode was activated."""
        self.send_discord(
            "Temp Playback Activated",
            f"Long download detected — streaming {file_count} ready files while download continues",
            color=COLOR_ROTATION_START
        )

    def notify_temp_playback_exited(self, playlist_names: list[str]):
        """Notify that temp playback mode exited and normal rotation resumed."""
        self.send_discord(
            "Temp Playback Complete",
            f"Download finished, switched to: **{', '.join(playlist_names)}**",
            color=COLOR_SUCCESS
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
            color=COLOR_INFO
        )

    def notify_video_transition(self, video_name: str, category: Optional[str] = None):
        """Notify about a video transition (optional, can be noisy)."""
        desc = f"**{video_name}**"
        if category:
            desc += f" ({category})"
        self.send_discord(
            "Video Transition",
            desc,
            color=COLOR_MUTED
        )

    def notify_automation_started(self):
        """Notify that the automation system has started."""
        self.send_discord(
            "Automation Started",
            "24/7 stream automation is online",
            color=COLOR_SUCCESS
        )

    def notify_automation_shutdown(self):
        """Notify that the automation system is shutting down."""
        self.send_discord(
            "Automation Shutting Down",
            "24/7 stream automation is going offline",
            color=COLOR_WARNING
        )

    def notify_streamer_live(self):
        """Notify that the streamer is live."""
        self.send_discord(
            "Streamer is LIVE!",
            "24/7 stream paused",
            color=COLOR_STREAM_LIVE
        )

    def notify_streamer_offline(self):
        """Notify that the streamer is offline."""
        self.send_discord(
            "Streamer is OFFLINE",
            "24/7 stream resumed",
            color=COLOR_SUCCESS
        )

    def notify_automation_error(self, error_message: str):
        """Notify about general automation errors."""
        self.send_discord(
            "Automation Error",
            f"Unexpected error: {error_message}",
            color=COLOR_ERROR
        )

    def notify_fallback_activated(self, tier: str):
        """Notify that fallback mode was activated due to download failures."""
        tier_labels = {
            "fallback_folder": "Playing backup content from fallback folder",
            "loop_remaining": "Looping remaining content (no new downloads)",
            "pause_screen": "No content available — stream paused",
        }
        desc = tier_labels.get(tier, tier)
        self.send_discord(
            "⚠ Fallback Mode Activated",
            f"Downloads are repeatedly failing.\n**Action:** {desc}\n\n"
            f"The system will automatically retry downloads every 5 minutes "
            f"and resume normal operation once a download succeeds.",
            color=COLOR_FALLBACK
        )

    def notify_fallback_deactivated(self):
        """Notify that fallback mode was deactivated (downloads recovered)."""
        self.send_discord(
            "✅ Fallback Mode Deactivated",
            "Downloads have recovered — resuming normal operation.",
            color=COLOR_SUCCESS
        )
