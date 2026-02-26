"""Dashboard command handler — web dashboard state & command routing.

Handles all interactions between the web dashboard and the automation
controller: building state snapshots, routing commands, applying settings,
playlist CRUD, manual pause/resume, and prepared rotation execution.
"""
import asyncio
import json
import logging
import os
import time
from typing import Optional, TYPE_CHECKING

from dotenv import set_key
from config.constants import (
    DEFAULT_VIDEO_FOLDER,
)

if TYPE_CHECKING:
    from controllers.automation_controller import AutomationController

logger = logging.getLogger(__name__)


class DashboardHandler:
    """Owns all web-dashboard-facing logic.

    Receives a back-reference to the :class:`AutomationController` so it can
    read/write shared state (playback monitor, OBS controller, managers, etc.)
    without duplicating it.
    """

    def __init__(self, ctrl: 'AutomationController') -> None:
        self.ctrl = ctrl

    # ── Convenience shortcuts ─────────────────────────────────────

    @property
    def _scene_pause(self) -> str:
        return self.ctrl._scene_pause

    @property
    def _scene_stream(self) -> str:
        return self.ctrl._scene_stream

    @property
    def _scene_rotation_screen(self) -> str:
        return self.ctrl._scene_rotation_screen

    @property
    def _vlc_source_name(self) -> str:
        return self.ctrl._vlc_source_name

    # ── Post-command state push ───────────────────────────────────

    async def _push_state_after_command(self, delay: float = 3.0) -> None:
        """Push an immediate state snapshot and schedule a delayed follow-up.

        Called after skip/rotate commands so the dashboard reflects
        changes without waiting for the next periodic push cycle.
        """
        if self.ctrl.web_dashboard:
            await self.ctrl.web_dashboard.push_state_now()
            asyncio.create_task(self.ctrl.web_dashboard.push_state_delayed(delay))

    # ── State snapshot ────────────────────────────────────────────

    def get_dashboard_state(self) -> dict:
        """Build a state snapshot for the web dashboard.

        Includes core status fields plus extended data for playlists,
        settings, queue, and platform connections pages.
        """
        ctrl = self.ctrl

        status = "offline"
        if ctrl.last_stream_status == "live":
            status = "paused"
        elif ctrl.obs_controller and ctrl.obs_controller.is_connected:
            status = "online"

        current_video: Optional[str] = None
        current_playlist: Optional[str] = None
        current_category: Optional[dict] = None

        if ctrl.playback_monitor:
            current_video = ctrl.playback_monitor.current_video_original_name

        session = ctrl.db.get_current_session()
        if session:
            playlists_json = session.get('playlists_selected')
            if playlists_json:
                try:
                    playlist_ids = json.loads(playlists_json) if isinstance(playlists_json, str) else playlists_json
                    names = []
                    for pid in playlist_ids:
                        p = ctrl.db.get_playlist(pid)
                        if p:
                            names.append(p['name'])
                    current_playlist = ", ".join(names) if names else None
                except Exception:
                    pass

        if ctrl.playback_monitor:
            current_category = ctrl.playback_monitor.get_category_for_current_video()

        # ── Extended data ──

        # Playlists from config (name, url, twitch_category, kick_category, enabled, priority)
        # Merge in last_played / play_count from the database.
        playlists = []
        try:
            # Build a lookup of DB stats keyed by playlist name
            db_stats: dict[str, dict] = {}
            try:
                # Also grab disabled ones so the dashboard shows stats for every playlist
                with ctrl.db._cursor() as _cur:
                    _cur.execute("SELECT name, last_played, play_count FROM playlists")
                    for row in _cur.fetchall():
                        db_stats[row["name"]] = {
                            "last_played": row["last_played"],
                            "play_count": row["play_count"] or 0,
                        }
            except Exception:
                pass

            for p in ctrl.config_manager.get_playlists():
                name = p.get("name", "")
                stats = db_stats.get(name, {})
                playlists.append({
                    "name": name,
                    "url": p.get("url", ""),
                    "twitch_category": p.get("twitch_category", "") or p.get("category", ""),
                    "kick_category": p.get("kick_category", "") or p.get("category", ""),
                    "enabled": p.get("enabled", True),
                    "priority": p.get("priority", 1),
                    "last_played": stats.get("last_played"),
                    "play_count": stats.get("play_count", 0),
                })
        except Exception:
            pass

        # Settings (all hot-swappable keys from settings.json)
        settings: dict = {}
        try:
            raw = ctrl.config_manager.get_settings()
            # Only expose dashboard-relevant keys (exclude folder paths)
            for key in (
                "stream_title_template",
                "ignore_streamer",
                "notify_video_transitions",
                "min_playlists_per_rotation",
                "max_playlists_per_rotation",
                "download_retry_attempts",
                "live_check_interval_seconds",
                "yt_dlp_use_cookies",
                "yt_dlp_browser_for_cookies",
                "yt_dlp_verbose",
            ):
                if key in raw:
                    settings[key] = raw[key]
        except Exception:
            pass

        # Video queue (files in the current rotation folder)
        queue: list[str] = []
        try:
            if ctrl.playback_monitor and ctrl.playback_monitor.video_folder:
                queue = ctrl.playback_monitor._get_video_files()
        except Exception:
            pass

        # Platform connections
        connections: dict = {}
        try:
            connections["obs"] = bool(ctrl.obs_controller and ctrl.obs_controller.is_connected)
            connections["twitch"] = bool(ctrl._env_twitch_client_id and ctrl._env_twitch_client_secret)
            connections["kick"] = bool(ctrl._env_kick_client_id and ctrl._env_kick_client_secret)
            connections["discord_webhook"] = bool(ctrl._env_discord_webhook_url)
            connections["twitch_enabled"] = bool(os.getenv("ENABLE_TWITCH", "").lower() == "true")
            connections["kick_enabled"] = bool(os.getenv("ENABLE_KICK", "").lower() == "true")
        except Exception:
            pass

        # Download status
        download_active = ctrl.download_manager.background_download_in_progress if ctrl.download_manager else False

        # Guard flags — determine whether skip/rotation are safe right now
        videos_remaining = len(queue) - (queue.index(ctrl.playback_monitor._current_video) + 1) if (
            ctrl.playback_monitor and ctrl.playback_monitor._current_video in queue
        ) else 0
        can_skip = videos_remaining > 0 or (not download_active and ctrl.next_prepared_playlists is not None)
        # Disallow trigger-rotation during a prepared rotation overlay
        can_trigger_rotation = not download_active and not ctrl.is_rotating and not ctrl._prepared_rotation_active

        # Prepared rotations state
        prepared_state = ctrl.prepared_rotation_manager.get_dashboard_state()

        # Environment configuration (for owner settings page)
        env_config = self._build_env_config()

        return {
            "status": status,
            "manual_pause": ctrl._manual_pause,
            "current_video": current_video,
            "current_playlist": current_playlist,
            "current_category": current_category,
            "obs_connected": bool(ctrl.obs_controller and ctrl.obs_controller.is_connected),
            "uptime_seconds": int(time.time() - ctrl._start_time),
            "playlists": playlists,
            "settings": settings,
            "queue": queue,
            "connections": connections,
            "download_active": download_active,
            "can_skip": can_skip,
            "can_trigger_rotation": can_trigger_rotation,
            "env_config": env_config,
            **prepared_state,
        }

    # ── Command router ────────────────────────────────────────────

    async def handle_command(self, command: dict) -> None:
        """Handle a command received from the web dashboard."""
        ctrl = self.ctrl
        action = command.get("action", "")
        payload = command.get("payload", {})

        if action == "skip_video":
            # Guard: only allow skip if there are more videos in the queue
            if ctrl.playback_monitor:
                queue = ctrl.playback_monitor._get_video_files()
                current = ctrl.playback_monitor._current_video
                idx = queue.index(current) if current and current in queue else -1
                videos_after = len(queue) - (idx + 1) if idx >= 0 else 0
                if videos_after <= 0 and ctrl.download_manager.background_download_in_progress:
                    logger.warning("Dashboard command: skip video REJECTED — last video and next rotation still downloading")
                    return
                if videos_after <= 0:
                    # Last video — VLC would just loop. Mark consumed so the
                    # main loop triggers rotation or prepared-rotation restore.
                    logger.info("Dashboard command: skip video — last video, marking all content consumed")
                    ctrl.playback_monitor._all_content_consumed = True
                    await self._push_state_after_command()
                    return
            logger.info("Dashboard command: skip video")
            if ctrl.obs_controller:
                try:
                    ctrl.obs_controller.obs_client.trigger_media_input_action(
                        name=self._vlc_source_name,
                        action="OBS_WEBSOCKET_MEDIA_INPUT_ACTION_NEXT",
                    )
                except Exception as e:
                    logger.warning(f"Failed to skip video via dashboard: {e}")
            await self._push_state_after_command()

        elif action == "trigger_rotation":
            # Guard: only allow if not already rotating and no download in progress
            if ctrl.download_manager.background_download_in_progress:
                logger.warning("Dashboard command: trigger rotation REJECTED — next rotation still downloading")
                return
            if ctrl.is_rotating:
                logger.warning("Dashboard command: trigger rotation REJECTED — rotation already in progress")
                return
            logger.info("Dashboard command: trigger rotation")
            # Force all-content-consumed so rotation triggers on next tick
            if ctrl.playback_monitor:
                ctrl.playback_monitor._all_content_consumed = True
            await self._push_state_after_command()

        elif action == "update_setting":
            key = payload.get("key")
            value = payload.get("value")
            if key:
                logger.info(f"Dashboard command: update setting {key}={value}")
                self._apply_setting(key, value)

        elif action == "add_playlist":
            logger.info(f"Dashboard command: add playlist {payload.get('name')}")
            self._playlist_add(payload)

        elif action == "update_playlist":
            logger.info(f"Dashboard command: update playlist {payload.get('name')}")
            self._playlist_update(payload)

        elif action == "remove_playlist":
            logger.info(f"Dashboard command: remove playlist {payload.get('name')}")
            self._playlist_remove(payload.get("name", ""))

        elif action == "rename_playlist":
            old_name = payload.get("old_name", "")
            new_name = payload.get("new_name", "")
            logger.info(f"Dashboard command: rename playlist '{old_name}' -> '{new_name}'")
            self._playlist_rename(old_name, new_name)

        elif action == "toggle_playlist":
            name = payload.get("name", "")
            enabled = payload.get("enabled")
            logger.info(f"Dashboard command: toggle playlist {name} -> {enabled}")
            self._playlist_toggle(name, enabled)

        elif action == "pause_stream":
            logger.info("Dashboard command: pause stream")
            self.manual_pause_stream()

        elif action == "resume_stream":
            logger.info("Dashboard command: resume stream")
            self.manual_resume_stream()

        # ── Prepared rotation commands ──

        elif action == "create_prepared_rotation":
            title = payload.get("title", "Untitled")
            playlists = payload.get("playlists", [])
            logger.info(f"Dashboard command: create prepared rotation '{title}' with {playlists}")
            ctrl.prepared_rotation_manager.create(title, playlists)

        elif action == "download_prepared_rotation":
            slug = payload.get("slug", "")
            folder = ctrl.prepared_rotation_manager.resolve_folder(slug)
            if not folder:
                logger.warning(f"download_prepared_rotation: invalid slug {slug!r}")
                return
            logger.info(f"Dashboard command: download prepared rotation '{slug}'")
            if not ctrl.prepared_rotation_manager.start_download(folder):
                logger.warning("download_prepared_rotation failed — check status or another download in progress")

        elif action == "cancel_prepared_download":
            slug = payload.get("slug", "")
            folder = ctrl.prepared_rotation_manager.resolve_folder(slug)
            if not folder:
                logger.warning(f"cancel_prepared_download: invalid slug {slug!r}")
                return
            logger.info(f"Dashboard command: cancel prepared download '{slug}'")
            ctrl.prepared_rotation_manager.cancel_download(folder)

        elif action == "execute_prepared_rotation":
            slug = payload.get("slug", "")
            folder = ctrl.prepared_rotation_manager.resolve_folder(slug)
            if not folder:
                logger.warning(f"execute_prepared_rotation: invalid slug {slug!r}")
                return
            restore_cursor = bool(payload.get("restore_cursor", False))
            logger.info(f"Dashboard command: execute prepared rotation '{slug}' (restore_cursor={restore_cursor})")
            await self.execute_prepared_rotation(folder, restore_cursor=restore_cursor)

        elif action == "delete_prepared_rotation":
            slug = payload.get("slug", "")
            folder = ctrl.prepared_rotation_manager.resolve_folder(slug)
            if not folder:
                logger.warning(f"delete_prepared_rotation: invalid slug {slug!r}")
                return
            logger.info(f"Dashboard command: delete prepared rotation '{slug}'")
            ctrl.prepared_rotation_manager.delete(folder)

        elif action == "clear_completed_prepared":
            logger.info("Dashboard command: clear completed prepared rotations")
            count = ctrl.prepared_rotation_manager.clear_completed()
            logger.info(f"Cleared {count} completed prepared rotations")

        elif action == "schedule_prepared_rotation":
            slug = payload.get("slug", "")
            folder = ctrl.prepared_rotation_manager.resolve_folder(slug)
            if not folder:
                logger.warning(f"schedule_prepared_rotation: invalid slug {slug!r}")
                return
            scheduled_at = payload.get("scheduled_at", "")
            logger.info(f"Dashboard command: schedule prepared rotation '{slug}' for {scheduled_at}")
            ctrl.prepared_rotation_manager.schedule(folder, scheduled_at)

        elif action == "cancel_prepared_schedule":
            slug = payload.get("slug", "")
            folder = ctrl.prepared_rotation_manager.resolve_folder(slug)
            if not folder:
                logger.warning(f"cancel_prepared_schedule: invalid slug {slug!r}")
                return
            logger.info(f"Dashboard command: cancel schedule for '{slug}'")
            ctrl.prepared_rotation_manager.cancel_schedule(folder)

        elif action == "reload_env":
            logger.info("Dashboard command: reload .env configuration")
            changed = ctrl.reload_env()
            if changed:
                safe = {k: ("****" if "SECRET" in k or "PASSWORD" in k or "API_KEY" in k else v) for k, v in changed.items()}
                logger.info(f"Environment reloaded — changed: {', '.join(safe.keys())}")
            else:
                logger.info("Environment reloaded — no changes")

        elif action == "update_env":
            key = payload.get("key", "")
            value = payload.get("value", "")
            if not key:
                logger.warning("update_env: missing key")
                return
            self._apply_env_var(key, str(value))

        else:
            logger.warning(f"Unknown dashboard command: {action}")

    # ── Settings ──────────────────────────────────────────────────

    def _apply_setting(self, key: str, value) -> None:
        """Write a single setting change to settings.json.

        Only whitelisted keys are accepted to prevent arbitrary file writes.
        After writing, the ConfigManager's mtime cache will pick up the
        change automatically on the next get_settings() call.
        """
        allowed_keys = {
            "stream_title_template",
            "ignore_streamer",
            "notify_video_transitions",
            "min_playlists_per_rotation",
            "max_playlists_per_rotation",
            "download_retry_attempts",
            "live_check_interval_seconds",
            "yt_dlp_use_cookies",
            "yt_dlp_browser_for_cookies",
            "yt_dlp_verbose",
        }

        if key not in allowed_keys:
            logger.warning(f"Dashboard tried to set disallowed key: {key}")
            return

        settings_path = self.ctrl.config_manager.settings_path
        try:
            with open(settings_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            data[key] = value
            with open(settings_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Setting '{key}' updated to {value!r} via dashboard")
        except Exception as e:
            logger.error(f"Failed to update setting '{key}': {e}")

    # ── Environment variables ─────────────────────────────────────

    # Keys the owner may read and/or write from the dashboard.
    # "safe" keys have their value sent to the frontend;
    # "secret" keys only report whether they are set (write-only).
    _ENV_SAFE_KEYS = {
        "OBS_HOST", "OBS_PORT",
        "SCENE_PAUSE", "SCENE_STREAM", "SCENE_ROTATION_SCREEN",
        "VLC_SOURCE_NAME",
        "ENABLE_TWITCH", "ENABLE_KICK",
        "TARGET_TWITCH_STREAMER", "TARGET_KICK_STREAMER",
    }
    _ENV_SECRET_KEYS = {
        "OBS_PASSWORD",
        "TWITCH_CLIENT_ID", "TWITCH_CLIENT_SECRET",
        "KICK_CLIENT_ID", "KICK_CLIENT_SECRET",
        "DISCORD_WEBHOOK_URL",
    }
    _ENV_ALLOWED_KEYS = _ENV_SAFE_KEYS | _ENV_SECRET_KEYS

    def _build_env_config(self) -> dict:
        """Return current env values for the settings page.

        Safe keys include their current value.  Secret keys only report
        ``True``/``False`` to indicate whether they are configured (the
        actual secret is never sent over the wire).
        """
        config: dict[str, dict] = {}
        for key in self._ENV_SAFE_KEYS:
            config[key] = {"value": os.getenv(key, ""), "secret": False}
        for key in self._ENV_SECRET_KEYS:
            config[key] = {"value": bool(os.getenv(key, "")), "secret": True}
        return config

    def _apply_env_var(self, key: str, value: str) -> None:
        """Write a single env var to the .env file, then trigger reload.

        Only whitelisted keys are accepted.
        """
        if key not in self._ENV_ALLOWED_KEYS:
            logger.warning(f"Dashboard tried to set disallowed env key: {key}")
            return

        from config.constants import _PROJECT_ROOT
        env_path = os.path.join(_PROJECT_ROOT, '.env')
        if not os.path.isfile(env_path):
            logger.error("update_env: .env file not found")
            return

        try:
            set_key(env_path, key, value)
            is_secret = key in self._ENV_SECRET_KEYS
            display = "****" if is_secret else value
            logger.info(f"Env var '{key}' written to .env → {display}")
        except Exception as e:
            logger.error(f"Failed to write env var '{key}': {e}")
            return

        # Apply immediately
        self.ctrl.reload_env()

    # ── Manual pause / resume ─────────────────────────────────────

    def manual_pause_stream(self) -> None:
        """Manually pause the stream from the dashboard.

        Reuses the same logic as the streamer-is-live pause: saves playback
        position, switches OBS to the pause scene, and sets the manual flag
        so the live checker won't auto-resume.
        """
        ctrl = self.ctrl

        if ctrl.last_stream_status == "live" and ctrl._manual_pause:
            logger.info("Stream is already manually paused")
            return

        # Save playback position
        if ctrl.current_session_id and ctrl.playback_monitor and ctrl.obs_controller:
            try:
                status = ctrl.obs_controller.get_media_input_status(self._vlc_source_name)
                if status and status.get('media_cursor') is not None:
                    current_video = ctrl.playback_monitor.current_video_original_name
                    ctrl.db.save_playback_position(
                        ctrl.current_session_id,
                        status['media_cursor'],
                        current_video
                    )
                    logger.info(f"Saved playback position before manual pause: {current_video} at {status['media_cursor']}ms")
            except Exception as e:
                logger.debug(f"Failed to save playback position before manual pause: {e}")

        # Switch to pause scene
        if ctrl.obs_controller:
            ctrl.obs_controller.switch_scene(self._scene_pause)

        ctrl.last_stream_status = "live"
        ctrl._manual_pause = True
        logger.info("Stream manually paused via dashboard")

    def manual_resume_stream(self) -> None:
        """Manually resume the stream from the dashboard.

        Switches OBS back to the stream scene, clears the manual pause flag,
        and queues a deferred seek to restore playback position.
        """
        ctrl = self.ctrl

        if ctrl.last_stream_status != "live":
            logger.info("Stream is not paused — nothing to resume")
            return

        # Switch to stream scene
        if ctrl.obs_controller:
            ctrl.obs_controller.switch_scene(self._scene_stream)

        # Restore playback position
        if ctrl.current_session_id:
            session = ctrl.db.get_current_session()
            if session:
                saved_video = session.get('playback_current_video')
                saved_cursor = session.get('playback_cursor_ms', 0)
                if saved_video and saved_cursor and saved_cursor > 0:
                    ctrl._pending_seek_ms = saved_cursor
                    ctrl._pending_seek_video = saved_video
                    logger.info(f"Pending seek after manual resume: {saved_video} at {saved_cursor}ms ({saved_cursor/1000:.1f}s)")

        ctrl.last_stream_status = "offline"
        ctrl._manual_pause = False
        ctrl._rotation_postpone_logged = False
        logger.info("Stream manually resumed via dashboard")

    # ── Prepared rotation execution ───────────────────────────────

    async def execute_prepared_rotation(self, folder: str, *, restore_cursor: bool = False) -> None:
        """Execute a prepared rotation by playing directly from its folder.

        Saves the current live playback state, renames videos in the prepared
        folder with playlist prefixes for correct ordering, switches OBS VLC
        source to the prepared folder, and reinitialises the playback monitor.

        Live/ and pending/ are left completely untouched.
        When all prepared content finishes, ``restore_after_prepared_rotation``
        puts VLC back on live/ and resumes where it left off.
        """
        ctrl = self.ctrl

        meta = ctrl.prepared_rotation_manager.begin_execution(folder)
        if not meta:
            logger.warning(f"Cannot execute prepared rotation at {folder}")
            return

        settings = ctrl.config_manager.get_settings()
        live_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)

        # ── 1. Save current playback state ──
        ctrl._saved_live_folder = live_folder
        ctrl._saved_live_video = None
        ctrl._saved_live_cursor_ms = 0
        ctrl._restore_cursor_after_prepared = restore_cursor

        if ctrl.playback_monitor and ctrl.obs_controller:
            try:
                # Use original_name (prefix-stripped) so it matches the
                # current_video_original_name used in the deferred-seek check.
                ctrl._saved_live_video = ctrl.playback_monitor.current_video_original_name
                status = ctrl.obs_controller.get_media_input_status(self._vlc_source_name)
                if status and status.get('media_cursor') is not None:
                    ctrl._saved_live_cursor_ms = int(status['media_cursor'])
            except Exception as e:
                logger.debug(f"Failed to save playback before prepared rotation: {e}")

        logger.info(
            f"Saved live state: video={ctrl._saved_live_video}, "
            f"cursor={ctrl._saved_live_cursor_ms}ms, folder={live_folder}"
        )

        # ── 2. Rename videos in prepared folder with playlist prefix ──
        playlist_names = meta.get("playlists", [])
        ctrl.playlist_manager.rename_videos_with_playlist_prefix(folder, playlist_names)

        # ── 3. Switch OBS to prepared folder ──
        if not ctrl.obs_controller:
            logger.error("No OBS controller — cannot execute prepared rotation")
            ctrl.prepared_rotation_manager.complete_execution()
            return

        # Brief rotation-screen while VLC reloads
        ctrl.obs_controller.switch_scene(self._scene_rotation_screen)
        await asyncio.sleep(1.5)

        success, _ = ctrl.obs_controller.update_vlc_source(self._vlc_source_name, folder)
        if not success:
            logger.error("Failed to point VLC at prepared rotation folder")
            ctrl.obs_controller.switch_scene(self._scene_stream)
            ctrl.prepared_rotation_manager.complete_execution()
            return

        # Back to stream scene
        await asyncio.sleep(0.5)
        ctrl.obs_controller.switch_scene(self._scene_stream)

        # ── 4. Reinitialise playback monitor on the prepared folder ──
        ctrl._initialize_playback_monitor(folder)
        # Don't delete videos during prepared rotation — they should be reusable
        if ctrl.playback_monitor:
            ctrl.playback_monitor._delete_on_transition = False

        # ── 5. Update stream title & category ──
        ctrl._prepared_rotation_active = True
        try:
            title = ctrl.playlist_manager.generate_stream_title(playlist_names)
            # Use first playlist's category as initial
            category = None
            if ctrl.playback_monitor:
                category = ctrl.playback_monitor.get_category_for_current_video()
            if not category and playlist_names:
                from utils.video_utils import resolve_playlist_categories
                for p in ctrl.config_manager.get_playlists():
                    if p.get('name') == playlist_names[0]:
                        category = resolve_playlist_categories(p)
                        break
            if ctrl.stream_manager:
                await ctrl.stream_manager.update_stream_info(title, category)
        except Exception as e:
            logger.warning(f"Failed to update stream metadata for prepared rotation: {e}")

        logger.info(f"Prepared rotation '{meta['title']}' now playing from {folder}")

    async def restore_after_prepared_rotation(self) -> None:
        """Restore live playback after a prepared rotation finishes.

        Puts VLC back on the live folder, seeks to the saved position, and
        reinitialises the playback monitor.  Marks the prepared rotation as
        completed.  Does NOT trigger a new rotation — the normal loop handles
        that if/when live content is consumed.
        """
        ctrl = self.ctrl

        logger.info("===== RESTORING LIVE PLAYBACK AFTER PREPARED ROTATION =====")

        ctrl._prepared_rotation_active = False

        # Mark the prepared rotation as completed
        ctrl.prepared_rotation_manager.complete_execution()

        live_folder = ctrl._saved_live_folder
        if not live_folder:
            settings = ctrl.config_manager.get_settings()
            live_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)

        if not ctrl.obs_controller:
            logger.error("No OBS controller — cannot restore live playback")
            return

        # Switch to rotation screen briefly
        ctrl.obs_controller.switch_scene(self._scene_rotation_screen)
        await asyncio.sleep(1.5)

        # Point VLC back at live/
        success, _ = ctrl.obs_controller.update_vlc_source(self._vlc_source_name, live_folder)
        if not success:
            logger.error("Failed to restore VLC source to live folder")
            ctrl.obs_controller.switch_scene(self._scene_stream)
            return

        await asyncio.sleep(0.5)
        ctrl.obs_controller.switch_scene(self._scene_stream)

        # Reinitialise playback monitor on live/
        ctrl._initialize_playback_monitor(live_folder)

        # Seek back to where we left off (if restore_cursor was requested)
        if ctrl._restore_cursor_after_prepared and ctrl._saved_live_video and ctrl._saved_live_cursor_ms > 0:
            ctrl._pending_seek_ms = ctrl._saved_live_cursor_ms
            ctrl._pending_seek_video = ctrl._saved_live_video
            logger.info(
                f"Queued deferred seek: {ctrl._saved_live_video} at "
                f"{ctrl._saved_live_cursor_ms}ms ({ctrl._saved_live_cursor_ms / 1000:.1f}s)"
            )
        elif not ctrl._restore_cursor_after_prepared:
            logger.info("Skipping cursor restore — restore_cursor was not requested for this execution")

        # Restore stream title & category from the active session
        try:
            session = ctrl.db.get_current_session()
            if session:
                stream_title = session.get('stream_title', '')
                category = None
                if ctrl.playback_monitor:
                    category = ctrl.playback_monitor.get_category_for_current_video()
                if ctrl.stream_manager and stream_title:
                    await ctrl.stream_manager.update_stream_info(stream_title, category)
                    logger.info(f"Restored stream title: {stream_title}")
        except Exception as e:
            logger.warning(f"Failed to restore stream metadata after prepared rotation: {e}")

        # Clear saved state
        ctrl._saved_live_video = None
        ctrl._saved_live_cursor_ms = 0
        ctrl._saved_live_folder = None
        ctrl._restore_cursor_after_prepared = False

        logger.info("Live playback restored after prepared rotation")

    # ── Playlist CRUD from dashboard ──────────────────────────────

    def _load_playlists_raw(self) -> dict:
        """Load the raw playlists.json file."""
        try:
            with open(self.ctrl.config_manager.config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load playlists.json: {e}")
            return {"playlists": []}

    def _save_playlists_raw(self, data: dict) -> bool:
        """Write the playlists.json file. Returns True on success."""
        try:
            with open(self.ctrl.config_manager.config_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info("playlists.json updated via dashboard")
            return True
        except Exception as e:
            logger.error(f"Failed to save playlists.json: {e}")
            return False

    def _playlist_add(self, payload: dict) -> None:
        """Add a new playlist from dashboard payload."""
        name = payload.get("name", "").strip()
        url = payload.get("url", "").strip()
        if not name or not url:
            logger.warning("Dashboard add_playlist missing name or url")
            return

        data = self._load_playlists_raw()
        # Prevent duplicates by name
        for p in data.get("playlists", []):
            if p.get("name", "").lower() == name.lower():
                logger.warning(f"Playlist '{name}' already exists — skipping add")
                return

        data.setdefault("playlists", []).append({
            "name": name,
            "url": url,
            "twitch_category": payload.get("twitch_category", "Just Chatting"),
            "kick_category": payload.get("kick_category", ""),
            "enabled": payload.get("enabled", True),
            "priority": payload.get("priority", 1),
        })
        self._save_playlists_raw(data)

    def _playlist_update(self, payload: dict) -> None:
        """Update an existing playlist's fields (matched by name)."""
        name = payload.get("name", "").strip()
        if not name:
            return

        data = self._load_playlists_raw()
        for p in data.get("playlists", []):
            if p.get("name", "").lower() == name.lower():
                if "url" in payload:
                    p["url"] = payload["url"]
                if "twitch_category" in payload:
                    p["twitch_category"] = payload["twitch_category"]
                if "kick_category" in payload:
                    p["kick_category"] = payload["kick_category"]
                if "enabled" in payload:
                    p["enabled"] = payload["enabled"]
                if "priority" in payload:
                    p["priority"] = payload["priority"]
                self._save_playlists_raw(data)
                return
        logger.warning(f"Playlist '{name}' not found for update")

    def _playlist_remove(self, name: str) -> None:
        """Remove a playlist by name."""
        if not name:
            return
        data = self._load_playlists_raw()
        original_len = len(data.get("playlists", []))
        data["playlists"] = [
            p for p in data.get("playlists", [])
            if p.get("name", "").lower() != name.lower()
        ]
        if len(data["playlists"]) < original_len:
            self._save_playlists_raw(data)
        else:
            logger.warning(f"Playlist '{name}' not found for removal")

    def _playlist_rename(self, old_name: str, new_name: str) -> None:
        """Rename a playlist: update playlists.json and cascade through DB."""
        old_name = old_name.strip()
        new_name = new_name.strip()
        if not old_name or not new_name:
            logger.warning("Dashboard rename_playlist missing old_name or new_name")
            return
        if old_name.lower() == new_name.lower():
            return  # no-op

        data = self._load_playlists_raw()
        # Check for name collision
        for p in data.get("playlists", []):
            if p.get("name", "").lower() == new_name.lower():
                logger.warning(f"Cannot rename to '{new_name}' — name already exists")
                return

        # Update playlists.json
        found = False
        for p in data.get("playlists", []):
            if p.get("name", "").lower() == old_name.lower():
                p["name"] = new_name
                found = True
                break
        if not found:
            logger.warning(f"Playlist '{old_name}' not found for rename")
            return
        self._save_playlists_raw(data)

        # Cascade through database
        db = self.ctrl.db
        if db:
            db.rename_playlist(old_name, new_name)
        logger.info(f"Playlist renamed: '{old_name}' -> '{new_name}'")

    def _playlist_toggle(self, name: str, enabled: bool | None) -> None:
        """Toggle a playlist's enabled state."""
        if not name:
            return
        data = self._load_playlists_raw()
        for p in data.get("playlists", []):
            if p.get("name", "").lower() == name.lower():
                p["enabled"] = enabled if enabled is not None else not p.get("enabled", True)
                self._save_playlists_raw(data)
                return
        logger.warning(f"Playlist '{name}' not found for toggle")
