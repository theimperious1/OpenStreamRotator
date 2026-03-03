"""Twitch EventSub WebSocket listener for channel raid events.

Connects to the Twitch EventSub WebSocket and subscribes to
``channel.raid`` for a specified broadcaster.  When the streamer
raids *any* channel, a callback fires so the automation controller
can unpause immediately — without waiting for the offline poll.

The listener runs as an ``asyncio.Task`` inside the main event loop.
It handles keepalive timeouts, ``session_reconnect`` messages, and
automatic token refresh via the existing TwitchTokenManager.

**Auth note:** ``channel.raid`` requires no special scopes, but the
EventSub *WebSocket transport* requires a user access token.  We
reuse the token already stored by TwitchUpdater (``channel:manage:broadcast``).
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional

import requests
import websockets
from urllib.parse import quote

from integrations.platforms.twitch import TwitchTokenManager

logger = logging.getLogger(__name__)

EVENTSUB_WS_URL = "wss://eventsub.wss.twitch.tv/ws"
HELIX_EVENTSUB_URL = "https://api.twitch.tv/helix/eventsub/subscriptions"
TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"

# Reconnect back-off limits
_MIN_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 60.0


class TwitchRaidListener:
    """Async EventSub WebSocket client that listens for ``channel.raid``.

    Parameters
    ----------
    client_id:
        Twitch application client ID.
    client_secret:
        Twitch application client secret.
    broadcaster_id:
        The Twitch user ID of the *24/7 channel* (whose user token we
        hold).  Used to look up stored OAuth tokens.
    streamer_user_id:
        The Twitch user ID of the *streamer* being monitored.  Raids
        originating from this user trigger the callback.
    on_raid:
        Awaitable or plain callback invoked when a raid is detected.
        Receives the raw event dict for logging purposes.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        broadcaster_id: str,
        streamer_user_id: str,
        on_raid: Callable,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.broadcaster_id = broadcaster_id
        self.streamer_user_id = streamer_user_id
        self._on_raid = on_raid

        self._token_manager = TwitchTokenManager()
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None

        self._ws: Optional[websockets.ClientConnection] = None
        self._session_id: Optional[str] = None
        self._keepalive_timeout: float = 30.0  # default; updated from welcome
        self._last_message_time: float = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ── Token helpers ────────────────────────────────────────────

    def _load_tokens(self) -> bool:
        """Load user tokens from the TwitchTokenManager store."""
        stored = self._token_manager.get_tokens(self.broadcaster_id)
        if stored:
            self._access_token = stored["access_token"]
            self._refresh_token = stored["refresh_token"]
            return True
        return False

    def _refresh_access_token(self) -> bool:
        """Refresh the user access token using the stored refresh token."""
        if not self._refresh_token:
            return False
        try:
            resp = requests.post(TWITCH_TOKEN_URL, data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
                "refresh_token": quote(self._refresh_token, safe=""),
            }, timeout=10)
            if resp.status_code in (400, 401):
                logger.error("Raid listener: refresh token invalid — re-auth needed")
                return False
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            self._refresh_token = data["refresh_token"]
            if self._access_token and self._refresh_token:
                self._token_manager.save_tokens(
                    self.broadcaster_id, self._access_token, self._refresh_token,
                )
            logger.info("Raid listener: user token refreshed")
            return True
        except Exception as e:
            logger.error(f"Raid listener: token refresh failed: {e}")
            return False

    # ── Subscription creation ────────────────────────────────────

    def _create_subscription(self) -> bool:
        """Create the ``channel.raid`` EventSub subscription via Helix API."""
        if not self._access_token or not self._session_id:
            return False

        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
        body = {
            "type": "channel.raid",
            "version": "1",
            "condition": {
                "from_broadcaster_user_id": self.streamer_user_id,
            },
            "transport": {
                "method": "websocket",
                "session_id": self._session_id,
            },
        }
        try:
            resp = requests.post(
                HELIX_EVENTSUB_URL, headers=headers,
                json=body, timeout=10,
            )
            if resp.status_code == 401:
                # Token expired — refresh and retry once
                if self._refresh_access_token():
                    headers["Authorization"] = f"Bearer {self._access_token}"
                    resp = requests.post(
                        HELIX_EVENTSUB_URL, headers=headers,
                        json=body, timeout=10,
                    )
                else:
                    return False
            if resp.status_code == 409:
                # Already subscribed (conflict) — that's fine
                logger.debug("Raid listener: subscription already exists (409)")
                return True
            resp.raise_for_status()
            logger.info(
                f"Raid listener: subscribed to channel.raid "
                f"(from_broadcaster={self.streamer_user_id})"
            )
            return True
        except Exception as e:
            logger.error(f"Raid listener: failed to create subscription: {e}")
            return False

    # ── WebSocket lifecycle ──────────────────────────────────────

    async def _connect_and_listen(self, url: str = EVENTSUB_WS_URL) -> None:
        """Connect to EventSub WS, subscribe, and listen for messages."""
        try:
            async with websockets.connect(url) as ws:
                self._ws = ws
                self._last_message_time = time.monotonic()

                async for raw_msg in ws:
                    self._last_message_time = time.monotonic()
                    msg = json.loads(raw_msg)
                    metadata = msg.get("metadata", {})
                    msg_type = metadata.get("message_type", "")

                    if msg_type == "session_welcome":
                        payload = msg.get("payload", {}).get("session", {})
                        self._session_id = payload.get("id")
                        ka = payload.get("keepalive_timeout_seconds", 30)
                        self._keepalive_timeout = float(ka) if ka else 30.0
                        logger.info(
                            f"Raid listener: connected (session={self._session_id}, "
                            f"keepalive={self._keepalive_timeout}s)"
                        )
                        # Create subscription now that we have a session
                        ok = await asyncio.to_thread(self._create_subscription)
                        if not ok:
                            logger.error("Raid listener: subscription creation failed")
                            return  # will trigger reconnect

                    elif msg_type == "session_keepalive":
                        pass  # just updates _last_message_time

                    elif msg_type == "session_reconnect":
                        new_url = (
                            msg.get("payload", {})
                            .get("session", {})
                            .get("reconnect_url")
                        )
                        if new_url:
                            logger.info(f"Raid listener: reconnecting to {new_url}")
                            # Recursively connect to the new URL; the old
                            # connection closes when this context exits.
                            await self._connect_and_listen(new_url)
                            return

                    elif msg_type == "notification":
                        sub_type = (
                            msg.get("payload", {})
                            .get("subscription", {})
                            .get("type")
                        )
                        if sub_type == "channel.raid":
                            event = msg.get("payload", {}).get("event", {})
                            from_name = event.get("from_broadcaster_user_name", "?")
                            to_name = event.get("to_broadcaster_user_name", "?")
                            viewers = event.get("viewers", 0)
                            logger.info(
                                f"RAID DETECTED: {from_name} → {to_name} "
                                f"({viewers} viewers)"
                            )
                            try:
                                result = self._on_raid(event)
                                if asyncio.iscoroutine(result):
                                    await result
                            except Exception as e:
                                logger.error(f"Raid callback error: {e}")

                    elif msg_type == "revocation":
                        reason = (
                            msg.get("payload", {})
                            .get("subscription", {})
                            .get("status", "unknown")
                        )
                        logger.warning(f"Raid listener: subscription revoked ({reason})")
                        return  # reconnect will re-subscribe

        except websockets.ConnectionClosedError as e:
            logger.warning(f"Raid listener: connection closed: {e}")
        except Exception as e:
            logger.error(f"Raid listener: WebSocket error: {e}")

    async def _keepalive_watchdog(self) -> None:
        """Kill the connection if no message arrives within the keepalive window."""
        while self._running:
            await asyncio.sleep(5)
            if not self._ws or self._last_message_time == 0:
                continue
            elapsed = time.monotonic() - self._last_message_time
            # Twitch recommends assuming dead after keepalive_timeout + ~10s grace
            if elapsed > self._keepalive_timeout + 10:
                logger.warning(
                    f"Raid listener: no message for {elapsed:.0f}s "
                    f"(limit {self._keepalive_timeout}s) — forcing reconnect"
                )
                if self._ws:
                    await self._ws.close()
                break

    async def _run_loop(self) -> None:
        """Main loop: connect, listen, reconnect on failure."""
        delay = _MIN_RECONNECT_DELAY
        while self._running:
            # Ensure we have tokens
            if not self._access_token:
                if not self._load_tokens():
                    logger.error(
                        "Raid listener: no user tokens available "
                        "(Twitch platform integration must be set up first)"
                    )
                    # Wait and retry — tokens might appear after TwitchUpdater auth
                    await asyncio.sleep(30)
                    continue

            # Run connection + watchdog concurrently
            watchdog = asyncio.create_task(self._keepalive_watchdog())
            try:
                await self._connect_and_listen()
            finally:
                watchdog.cancel()
                try:
                    await watchdog
                except asyncio.CancelledError:
                    pass

            if not self._running:
                break

            # Reconnect with exponential back-off
            logger.info(f"Raid listener: reconnecting in {delay:.0f}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_RECONNECT_DELAY)

            # Refresh token before reconnecting
            await asyncio.to_thread(self._refresh_access_token)

        logger.info("Raid listener: stopped")

    # ── Public API ───────────────────────────────────────────────

    def start(self) -> None:
        """Start the listener as a background asyncio task.

        Must be called from within a running event loop.
        """
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Raid listener: task started")

    async def stop(self) -> None:
        """Gracefully stop the listener."""
        if not self._running:
            return
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Raid listener: stopped")
