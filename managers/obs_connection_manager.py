"""OBS WebSocket connection lifecycle manager.

Handles connect, exponential-backoff reconnect, and graceful
disconnect for the OBS WebSocket client and controller.
"""
import logging
from threading import Event
from typing import Optional

import obsws_python as obs

from controllers.obs_controller import OBSController

logger = logging.getLogger(__name__)


class OBSConnectionManager:
    """Manages the OBS WebSocket connection lifecycle.

    Handles initial connection, exponential-backoff reconnection, and
    graceful disconnection.  Exposes the live ``OBSController`` instance
    for the rest of the application to use.
    """

    def __init__(
        self,
        host: str,
        port: int,
        password: str,
        shutdown_event: Event,
        timeout: int = 3,
    ):
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout
        self._shutdown_event = shutdown_event

        self.client: Optional[obs.ReqClient] = None
        self.controller: Optional[OBSController] = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Establish a fresh OBS WebSocket connection.

        Returns:
            True if connected successfully, False otherwise.
        """
        try:
            self.client = obs.ReqClient(
                host=self.host,
                port=self.port,
                password=self.password,
                timeout=self.timeout,
            )
            self.controller = OBSController(self.client)
            logger.info("Connected to OBS successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to OBS: {e}")
            return False

    def reconnect(
        self,
        max_retries: int = 0,
        base_delay: float = 2.0,
        max_delay: float = 60.0,
    ) -> bool:
        """Reconnect to OBS with exponential backoff.

        Args:
            max_retries: Maximum retry attempts (0 = unlimited until shutdown).
            base_delay:  Starting delay in seconds between retries.
            max_delay:   Maximum delay cap in seconds.

        Returns:
            True if reconnected, False if shutdown was requested or retries
            exhausted.
        """
        attempt = 0
        delay = base_delay
        while not self._shutdown_event.is_set():
            attempt += 1
            if max_retries and attempt > max_retries:
                logger.error(f"OBS reconnect failed after {max_retries} attempts")
                return False
            logger.info(f"OBS reconnect attempt {attempt} (waiting {delay:.0f}s)...")
            # Use shutdown event for interruptible sleep
            if self._shutdown_event.wait(timeout=delay):
                return False  # Shutdown requested
            if self.connect():
                logger.info(f"OBS reconnected after {attempt} attempt(s)")
                return True
            delay = min(delay * 2, max_delay)
        return False

    def disconnect(self) -> None:
        """Disconnect from OBS (call only on shutdown)."""
        if self.client:
            try:
                self.client.disconnect()
            except Exception as e:
                logger.debug(f"OBS disconnect warning (non-critical): {e}")
            self.client = None
