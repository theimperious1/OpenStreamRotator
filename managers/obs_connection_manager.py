"""OBS WebSocket connection lifecycle manager.

Handles connect, exponential-backoff reconnect, and graceful
disconnect for the OBS WebSocket client (ReqClient) and controller,
plus the EventClient used for media-playback transition detection.
"""
import logging
from queue import Queue
from threading import Event
from typing import Optional

import obsws_python as obs

from controllers.obs_controller import OBSController

logger = logging.getLogger(__name__)


class OBSConnectionManager:

    def __init__(
        self,
        host: str,
        port: int,
        password: str,
        shutdown_event: Event,
        timeout: int = 3,
        vlc_source_name: str = "OSR Playlist",
    ):
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout
        self._shutdown_event = shutdown_event
        self._vlc_source_name = vlc_source_name

        self.client: Optional[obs.ReqClient] = None
        self.controller: Optional[OBSController] = None

        # EventClient for media transition events
        self._event_client: Optional[obs.EventClient] = None
        # Thread-safe queue consumed by PlaybackMonitor.check()
        self.media_event_queue: Queue = Queue()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Establish a fresh OBS WebSocket connection (Req + Event clients).

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

            # Connect the EventClient for media transition events
            self._connect_event_client()

            logger.info("Connected to OBS successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to OBS: {e}")
            return False

    def _connect_event_client(self) -> None:
        """Create an EventClient and register media-event callbacks.

        The callbacks push lightweight strings (``"ended"`` / ``"started"``)
        into ``media_event_queue`` which the ``PlaybackMonitor`` drains on
        each tick.
        """
        # Tear down previous EventClient if any
        self._disconnect_event_client()

        try:
            self._event_client = obs.EventClient(
                host=self.host,
                port=self.port,
                password=self.password,
                timeout=self.timeout,
            )

            def on_media_input_playback_ended(data):  # type: ignore[no-untyped-def]
                if data.input_name != self._vlc_source_name:
                    logger.debug(f"OBS event: MediaInputPlaybackEnded ignored (source: {data.input_name})")
                    return
                self.media_event_queue.put("ended")
                logger.debug(f"OBS event: MediaInputPlaybackEnded ({data.input_name})")

            def on_media_input_playback_started(data):  # type: ignore[no-untyped-def]
                if data.input_name != self._vlc_source_name:
                    logger.debug(f"OBS event: MediaInputPlaybackStarted ignored (source: {data.input_name})")
                    return
                self.media_event_queue.put("started")
                logger.debug(f"OBS event: MediaInputPlaybackStarted ({data.input_name})")

            self._event_client.callback.register([
                on_media_input_playback_ended,
                on_media_input_playback_started,
            ])

            logger.info("OBS EventClient connected — listening for media events")
        except Exception as e:
            logger.warning(f"Failed to connect OBS EventClient (media events unavailable): {e}")
            self._event_client = None

    def _disconnect_event_client(self) -> None:
        """Cleanly tear down the EventClient."""
        if self._event_client:
            try:
                self._event_client.disconnect()
            except Exception:
                pass
            self._event_client = None

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
        self._disconnect_event_client()
        if self.client:
            try:
                self.client.disconnect()
            except Exception as e:
                logger.debug(f"OBS disconnect warning (non-critical): {e}")
            self.client = None
