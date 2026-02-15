"""WebSocket client for the OSR Web Dashboard.

Connects to the web backend via WebSocket and:
- Sends periodic state snapshots (current video, playlist, OBS status, uptime)
- Forwards log entries from the Python logger in real-time
- Receives commands from the dashboard (skip_video, trigger_rotation, etc.)
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Callable, Awaitable, Any

import websockets
from websockets.asyncio.client import ClientConnection

logger = logging.getLogger(__name__)

# Suppress noisy websockets library logging
logging.getLogger("websockets").setLevel(logging.WARNING)

# Connection settings
_RECONNECT_DELAY_BASE = 5      # seconds, doubles up to max
_RECONNECT_DELAY_MAX = 60
_STATE_PUSH_INTERVAL = 5        # seconds between state snapshots
_HEARTBEAT_INTERVAL = 30        # seconds between pings


class DashboardLogHandler(logging.Handler):
    """Logging handler that queues log records for forwarding to the dashboard."""

    def __init__(self, max_queue: int = 500):
        super().__init__()
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=max_queue)

    def emit(self, record: logging.LogRecord) -> None:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": self.format(record),
        }
        try:
            self._queue.put_nowait(entry)
        except asyncio.QueueFull:
            # Drop oldest to make room
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(entry)
            except asyncio.QueueFull:
                pass

    async def drain(self) -> list[dict]:
        """Return all queued log entries without blocking."""
        entries: list[dict] = []
        while not self._queue.empty():
            try:
                entries.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return entries


class WebDashboardClient:
    """Async WebSocket client connecting an OSR instance to the web dashboard.

    Usage:
        client = WebDashboardClient(
            api_key="...",
            state_provider=my_state_callback,
            command_handler=my_command_callback,
        )
        # In your async main loop:
        asyncio.create_task(client.run())
        # On shutdown:
        await client.close()
    """

    def __init__(
        self,
        api_key: str,
        state_provider: Callable[[], dict],
        command_handler: Callable[[dict], Awaitable[None]],
        server_url: str = "ws://localhost:8000",
    ):
        """
        Args:
            api_key: The OSR instance API key (from the web dashboard team page).
            state_provider: A callable returning the current state dict to push.
            command_handler: An async callable handling incoming commands from the dashboard.
            server_url: WebSocket server base URL (ws:// or wss://).
        """
        self._api_key = api_key
        self._state_provider = state_provider
        self._command_handler = command_handler
        self._server_url = server_url.rstrip("/")
        self._ws: Optional[ClientConnection] = None
        self._running = False
        self._connected = False

        # Set up the log handler to capture and forward logs
        self._log_handler = DashboardLogHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%H:%M:%S",
        )
        self._log_handler.setFormatter(formatter)
        self._log_handler.setLevel(logging.INFO)

    @property
    def connected(self) -> bool:
        return self._connected

    def attach_logger(self) -> None:
        """Attach the log handler to the root logger so all logs are captured."""
        root = logging.getLogger()
        if self._log_handler not in root.handlers:
            root.addHandler(self._log_handler)
            logger.debug("Dashboard log handler attached to root logger")

    def detach_logger(self) -> None:
        """Remove the log handler from the root logger."""
        root = logging.getLogger()
        if self._log_handler in root.handlers:
            root.removeHandler(self._log_handler)

    async def run(self) -> None:
        """Main loop: connect, push state, forward logs, handle commands.

        Automatically reconnects with exponential backoff on disconnection.
        Runs until close() is called.
        """
        self._running = True
        self.attach_logger()
        reconnect_delay = _RECONNECT_DELAY_BASE

        while self._running:
            try:
                url = f"{self._server_url}/ws/osr/{self._api_key}"
                logger.info(f"Connecting to web dashboard at {self._server_url}...")

                async with websockets.connect(url, ping_interval=_HEARTBEAT_INTERVAL) as ws:
                    self._ws = ws
                    self._connected = True
                    reconnect_delay = _RECONNECT_DELAY_BASE  # reset on success
                    logger.info("Connected to web dashboard")

                    # Run send and receive loops concurrently
                    await asyncio.gather(
                        self._send_loop(ws),
                        self._recv_loop(ws),
                    )

            except (websockets.ConnectionClosed, websockets.InvalidStatus) as e:
                if not self._running:
                    break
                logger.warning(f"Dashboard connection lost: {e}")
            except OSError as e:
                if not self._running:
                    break
                logger.warning(f"Dashboard connection failed: {e}")
            except Exception as e:
                if not self._running:
                    break
                logger.error(f"Unexpected dashboard client error: {e}", exc_info=True)
            finally:
                self._ws = None
                self._connected = False

            if not self._running:
                break

            logger.info(f"Reconnecting to dashboard in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, _RECONNECT_DELAY_MAX)

        self.detach_logger()
        logger.info("Dashboard client stopped")

    async def _send_loop(self, ws: ClientConnection) -> None:
        """Periodically push state snapshots and forward queued log entries."""
        last_state_push = 0.0

        while self._running:
            now = time.monotonic()

            # Push state snapshot
            if now - last_state_push >= _STATE_PUSH_INTERVAL:
                try:
                    state = self._state_provider()
                    await ws.send(json.dumps({"type": "state", "data": state}))
                    last_state_push = now
                except Exception as e:
                    logger.debug(f"Failed to send state: {e}")
                    return  # Let outer loop handle reconnect

            # Drain and forward log entries
            entries = await self._log_handler.drain()
            for entry in entries:
                try:
                    await ws.send(json.dumps({"type": "log", "data": entry}))
                except Exception:
                    return

            await asyncio.sleep(0.5)  # Small sleep to batch logs efficiently

    async def _recv_loop(self, ws: ClientConnection) -> None:
        """Listen for commands from the dashboard and dispatch them."""
        logger.info("Dashboard recv loop started, listening for commands...")
        async for raw in ws:
            if not self._running:
                break
            try:
                raw_str = raw if isinstance(raw, str) else raw.decode("utf-8") # type: ignore
                msg = json.loads(raw_str)
                action = msg.get("action")
                logger.info(f"Dashboard message received: {msg}")
                if action:
                    logger.info(f"Dispatching dashboard command: {action}")
                    await self._command_handler(msg)
                else:
                    logger.debug(f"Dashboard message has no 'action' field: {msg}")
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON from dashboard: {raw!r}")
            except Exception as e:
                logger.error(f"Error handling dashboard command: {e}", exc_info=True)

    async def close(self) -> None:
        """Gracefully shut down the client."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        self.detach_logger()
