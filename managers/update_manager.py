"""Update manager for automated OpenStreamRotator version checking and installation.

Polls GitHub releases API for new versions and handles critical yt-dlp fixes
with fallback mode awareness. Only auto-updates critical releases when fallback
is active (stream already compromised, so restart won't make things worse).
"""
import logging
import urllib.request
import json
from typing import Optional, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class UpdateManager:
    """Check for and manage OSR updates from GitHub releases."""

    GITHUB_API_URL = "https://api.github.com/repos/theimperious1/OpenStreamRotator/releases/latest"
    CRITICAL_KEYWORD = "[yt-dlp-update]"  # Must be in release name or body to be auto-applied

    def __init__(self, current_version: str, notification_service=None):
        """Initialize update manager.

        Args:
            current_version: Current OSR version (e.g., "1.1.1")
            notification_service: Optional NotificationService for Discord alerts
        """
        self.current_version = current_version
        self.notification_service = notification_service
        
        # State tracking
        self._last_check_time: Optional[datetime] = None
        self._check_interval = timedelta(minutes=30)
        self._available_version: Optional[str] = None
        self._available_is_critical = False
        self._update_pending = False
        self._update_suppressed_until: Optional[datetime] = None

    async def check_for_updates(self) -> bool:
        """Check GitHub for new releases.

        Returns True if an update is available and should be applied.
        """
        # Rate limit checks to every 30 minutes
        now = datetime.now()
        if self._last_check_time and (now - self._last_check_time) < self._check_interval:
            return False

        self._last_check_time = now

        try:
            # Fetch latest release from GitHub
            release_data = await self._fetch_latest_release()
            if not release_data:
                return False

            tag_name = release_data.get("tag_name", "").lstrip("v")
            if not tag_name:
                logger.warning("Latest release has no tag name")
                return False

            # Parse version (e.g., "1.2.0" from "v1.2.0")
            if self._is_newer_version(tag_name):
                self._available_version = tag_name
                release_name = release_data.get("name", "")
                release_body = release_data.get("body", "")
                is_critical = self.CRITICAL_KEYWORD.lower() in (
                    release_name + " " + release_body
                ).lower()

                self._available_is_critical = is_critical
                logger.info(
                    f"Update available: v{tag_name} "
                    f"({'CRITICAL' if is_critical else 'normal'})"
                )
                return True

        except Exception as e:
            logger.warning(f"Failed to check for updates: {e}")

        return False

    async def should_auto_install(self, fallback_active: bool) -> bool:
        """Determine if update should auto-install.

        Only auto-installs critical updates when fallback is active
        (stream already compromised, restart helps more than hurts).

        Args:
            fallback_active: True if stream is in fallback mode

        Returns:
            True if update should proceed immediately
        """
        if self._update_pending and self._available_is_critical and fallback_active:
            logger.info(
                f"Auto-installing critical update v{self._available_version} "
                f"(fallback mode active)"
            )
            return True

        if self._update_pending:
            if self._available_is_critical:
                logger.info(
                    f"Critical update v{self._available_version} available "
                    f"(will auto-install if fallback activates)"
                )
            else:
                logger.info(
                    f"Normal update v{self._available_version} available "
                    f"(manual restart recommended)"
                )

        return False

    def get_update_info(self) -> Optional[Dict]:
        """Get info about available update.

        Returns:
            Dict with 'version', 'is_critical' keys, or None if no update
        """
        if self._available_version:
            return {
                "version": self._available_version,
                "is_critical": self._available_is_critical,
                "keyword": self.CRITICAL_KEYWORD if self._available_is_critical else "",
            }
        return None

    async def _fetch_latest_release(self) -> Optional[Dict]:
        """Fetch latest release info from GitHub API.

        Returns:
            Release data dict or None on error
        """
        try:
            with urllib.request.urlopen(self.GITHUB_API_URL, timeout=10) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode())
                    return data
        except Exception as e:
            logger.debug(f"GitHub API call failed: {e}")
        return None

    def _is_newer_version(self, remote_version: str) -> bool:
        """Compare version strings (e.g., "1.2.0" > "1.1.5").

        Args:
            remote_version: Version string to compare (e.g., "1.2.0")

        Returns:
            True if remote is newer than current
        """
        try:
            local_parts = tuple(map(int, self.current_version.split(".")))
            remote_parts = tuple(map(int, remote_version.split(".")))

            # Pad shorter version with zeros
            max_len = max(len(local_parts), len(remote_parts))
            local_parts = local_parts + (0,) * (max_len - len(local_parts))
            remote_parts = remote_parts + (0,) * (max_len - len(remote_parts))

            return remote_parts > local_parts
        except Exception as e:
            logger.warning(f"Failed to compare versions {self.current_version} vs {remote_version}: {e}")
            return False

    def mark_update_available(self):
        """Mark that an update is ready to be considered for installation."""
        self._update_pending = True

    def reset_update_state(self):
        """Reset update tracking (called after successful update or manual dismissal)."""
        self._available_version = None
        self._available_is_critical = False
        self._update_pending = False

    def suppress_update_until(self, seconds: int):
        """Suppress update notifications for N seconds.

        Args:
            seconds: Duration to suppress in seconds
        """
        self._update_suppressed_until = datetime.now() + timedelta(seconds=seconds)
        logger.info(f"Update notifications suppressed for {seconds}s")

    def is_suppressed(self) -> bool:
        """Check if update notifications are currently suppressed."""
        if self._update_suppressed_until:
            if datetime.now() < self._update_suppressed_until:
                return True
            self._update_suppressed_until = None
        return False
