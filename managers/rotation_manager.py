"""Rotation session lifecycle management.

Owns session creation, content switching, normal rotation flow, and
session resume (including crash recovery).  Works in tandem with the
AutomationController which provides access to component managers and
shared state.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from controllers.automation_controller import AutomationController

from core.database import DatabaseManager
from config.constants import DEFAULT_VIDEO_FOLDER, DEFAULT_NEXT_ROTATION_FOLDER

logger = logging.getLogger(__name__)


class RotationManager:
    """Uses a back-reference to the controller for shared state access."""

    def __init__(
        self,
        ctrl: AutomationController,
        *,
        scene_stream: str,
        scene_pause: str,
        scene_rotation_screen: str,
        vlc_source_name: str,
    ):
        self._ctrl = ctrl
        self._scene_stream = scene_stream
        self._scene_pause = scene_pause
        self._scene_rotation = scene_rotation_screen
        self._vlc_source = vlc_source_name

    # ------------------------------------------------------------------
    # Session creation
    # ------------------------------------------------------------------

    async def start_session(self, manual_playlists=None) -> bool:
        """Start a new rotation session."""
        ctrl = self._ctrl

        logger.info("Starting new rotation session...")

        settings = ctrl.config_manager.get_settings()
        next_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)

        # Use prepared playlists or select new ones
        using_prepared = False
        if ctrl.next_prepared_playlists:
            playlists = ctrl.next_prepared_playlists
            ctrl.next_prepared_playlists = None
            using_prepared = True
            logger.info(f"Using prepared playlists: {[p['name'] for p in playlists]}")
        else:
            playlists = ctrl.playlist_manager.select_playlists_for_rotation(manual_playlists)

        if not playlists:
            logger.error("No playlists selected for rotation")
            ctrl.notification_service.notify_rotation_error("No playlists available")
            return False

        # Download if not already prepared
        if not using_prepared:
            logger.info(f"Downloading {len(playlists)} playlists...")
            ctrl.notification_service.notify_rotation_started([p['name'] for p in playlists])

            # Check for verbose yt-dlp logging
            settings = ctrl.config_manager.get_settings()
            verbose_download = settings.get('yt_dlp_verbose', False)

            download_result = await asyncio.get_event_loop().run_in_executor(
                ctrl.download_manager.executor,
                lambda: ctrl.playlist_manager.download_playlists(playlists, next_folder, verbose=verbose_download)
            )
            total_duration_seconds = download_result.get('total_duration_seconds', 0)

            if not download_result.get('success'):
                logger.error("Failed to download all playlists")
                ctrl.notification_service.notify_download_warning(
                    "Some playlists failed to download, continuing with available content"
                )
        else:
            logger.info("Using pre-downloaded playlists, skipping download step")
            total_duration_seconds = 0
            for playlist in playlists:
                playlist_id = playlist.get('id')
                if playlist_id:
                    videos = ctrl.db.get_videos_by_playlist(playlist_id)
                    for video in videos:
                        total_duration_seconds += video.get('duration_seconds', 0)

        # Validate and create session
        if not ctrl.playlist_manager.validate_downloads(next_folder):
            logger.error("Download validation failed")
            ctrl.notification_service.notify_rotation_error("Download validation failed")
            return False

        playlist_names = [p['name'] for p in playlists]
        stream_title = ctrl.playlist_manager.generate_stream_title(playlist_names)

        logger.info(f"Total rotation duration: {total_duration_seconds}s (~{total_duration_seconds // 60} minutes)")

        playlist_ids = [p['id'] for p in playlists]
        ctrl.current_session_id = ctrl.db.create_rotation_session(
            playlist_ids, stream_title,
            total_duration_seconds=total_duration_seconds
        )
        # Keep temp playback handler in sync
        if ctrl.temp_playback_handler:
            ctrl.temp_playback_handler.set_session_id(ctrl.current_session_id)

        logger.info("Rotation session prepared, ready to switch")
        return True

    # ------------------------------------------------------------------
    # Content switching
    # ------------------------------------------------------------------

    async def execute_content_switch(self) -> bool:
        """Execute content switch using handler."""
        ctrl = self._ctrl
        assert ctrl.content_switch_handler is not None, "Content switch handler not initialized"
        assert ctrl.stream_manager is not None, "Stream manager not initialized"

        # Safety guard: never execute content switch while temp playback is active.
        # Temp playback streams from the pending folder — switching content would
        # destroy the live folder and disrupt playback.
        if ctrl.temp_playback_handler and ctrl.temp_playback_handler.is_active:
            logger.error("execute_content_switch called while temp playback is active — aborting")
            return False

        if not ctrl.obs_controller:
            logger.error("OBS controller not initialized")
            return False

        logger.info(f"Executing content switch")
        ctrl.is_rotating = True

        settings = ctrl.config_manager.get_settings()
        current_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)
        next_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)

        try:
            # Prepare for switch
            if not ctrl.content_switch_handler.prepare_for_switch(self._scene_rotation, self._vlc_source):
                logger.error("Failed to prepare for content switch")
                ctrl.is_rotating = False
                return False

            # Execute folder operations
            if not ctrl.content_switch_handler.execute_switch(
                current_folder, next_folder
            ):
                ctrl.is_rotating = False
                return False

            # Process any queued videos from downloads so they're in database before rename/category lookup
            ctrl.download_manager.process_video_registration_queue()

            # Rename videos with playlist ordering prefix (01_, 02_, etc.)
            # so alphabetical ordering groups by playlist
            try:
                session = ctrl.db.get_current_session()
                if session:
                    playlists_selected = session.get('playlists_selected', '')
                    if playlists_selected:
                        playlist_ids = json.loads(playlists_selected)
                        playlists = ctrl.playlist_manager.get_playlists_by_ids(playlist_ids)
                        playlist_order = [p['name'] for p in playlists]
                        ctrl.playlist_manager.rename_videos_with_playlist_prefix(current_folder, playlist_order)
            except Exception as e:
                logger.warning(f"Failed to rename videos with prefix: {e}")

            # Finalize (update VLC + switch scene)
            target_scene = self._scene_pause if ctrl.last_stream_status == "live" else self._scene_stream
            finalize_success, vlc_playlist = ctrl.content_switch_handler.finalize_switch(
                current_folder, self._vlc_source, target_scene, self._scene_stream, ctrl.last_stream_status
            )
            if not finalize_success:
                ctrl.is_rotating = False
                return False

            # Initialize file lock monitor for this rotation
            ctrl._initialize_file_lock_monitor(current_folder)

            # Update stream title and category based on current video
            try:
                session = ctrl.db.get_current_session()
                if session:
                    stream_title = session.get('stream_title', '')

                    # Get category from first video in rotation
                    category = None
                    if ctrl.file_lock_monitor:
                        category = ctrl.file_lock_monitor.get_category_for_current_video()

                    # Fallback: get category from first playlist
                    if not category and ctrl.content_switch_handler:
                        category = ctrl.content_switch_handler.get_initial_rotation_category(
                            current_folder, ctrl.playlist_manager
                        )

                    await ctrl.stream_manager.update_stream_info(stream_title, category)
                    logger.info(f"Updated stream: title='{stream_title}', category='{category}'")
            except Exception as e:
                logger.warning(f"Failed to update stream metadata: {e}")

            # If temp playback was active, the normal rotation has completed the consolidation
            # Complete temp playback cleanup properly
            if ctrl.temp_playback_handler and ctrl.temp_playback_handler.is_active:
                await ctrl.temp_playback_handler.cleanup_after_rotation()

            # Clean up temporary download files from the previous rotation
            # Safe to do now that content switch is complete
            settings = ctrl.config_manager.get_settings()
            pending_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
            ctrl.playlist_manager.cleanup_temp_downloads(pending_folder)

            # Mark playlists as played so the selector rotates through them
            try:
                session = ctrl.db.get_current_session()
                if session:
                    playlists_selected = session.get('playlists_selected', '')
                    if playlists_selected:
                        pids = json.loads(playlists_selected)
                        for pid in pids:
                            ctrl.db.update_playlist_played(pid)
                        pls = ctrl.playlist_manager.get_playlists_by_ids(pids)
                        ctrl.notification_service.notify_rotation_switched([p['name'] for p in pls])
            except Exception:
                pass  # Non-critical

            ctrl.is_rotating = False
            logger.info("Content switch completed successfully")
            return True

        except Exception as e:
            logger.error(f"Content switch failed: {e}", exc_info=True)
            ctrl.is_rotating = False
            return False

    # ------------------------------------------------------------------
    # Normal rotation handling
    # ------------------------------------------------------------------

    async def handle_normal_rotation(self):
        """Handle normal rotation completion."""
        ctrl = self._ctrl

        # Don't rotate if stream is live
        if ctrl.last_stream_status == "live":
            if not ctrl._rotation_postpone_logged:
                logger.info("Stream is live, postponing rotation until stream goes offline")
                ctrl._rotation_postpone_logged = True
            return

        if ctrl.current_session_id:
            # Before ending session, get the current playlist info for audit trail
            session = ctrl.db.get_current_session()
            current_playlist_names = []

            if session:
                try:
                    playlists_selected = session.get('playlists_selected', '')
                    if playlists_selected:
                        playlist_ids = json.loads(playlists_selected)
                        playlists = ctrl.playlist_manager.get_playlists_by_ids(playlist_ids)
                        if playlists:
                            current_playlist_names = [p['name'] for p in playlists]
                            # Record what was just played
                            ctrl.db.set_current_playlists(ctrl.current_session_id, current_playlist_names)
                            logger.info(f"Recorded current playlists: {current_playlist_names}")
                except Exception as e:
                    logger.warning(f"Failed to record current playlists: {e}")

            ctrl.db.end_session(ctrl.current_session_id)

        if await self.start_session():
            # Reset download flag when starting new rotation
            ctrl.download_manager.downloads_triggered_this_rotation = False
            ctrl.download_manager.background_download_in_progress = False
            await self.execute_content_switch()

    # ------------------------------------------------------------------
    # Session resume (crash recovery)
    # ------------------------------------------------------------------

    async def resume_existing_session(self, session: dict, settings: dict):
        """Resume an existing session on startup (including crash recovery).

        Handles temp-playback restoration, prepared-playlist validation,
        playback position recovery, and stream-title restoration.
        """
        ctrl = self._ctrl

        ctrl.current_session_id = session['id']
        if ctrl.temp_playback_handler:
            ctrl.temp_playback_handler.set_session_id(ctrl.current_session_id)
        logger.info(f"Resuming session {ctrl.current_session_id}")

        # Notify crash recovery / session resume
        saved_video = session.get('playback_current_video')
        saved_cursor = session.get('playback_cursor_ms', 0)
        assert ctrl.current_session_id is not None
        ctrl.notification_service.notify_session_resumed(
            ctrl.current_session_id,
            video=saved_video,
            cursor_s=(saved_cursor / 1000) if saved_cursor else None
        )

        # Check for temp playback state that needs to be restored (crash recovery)
        temp_state = ctrl.db.get_temp_playback_state(session['id'])
        temp_playback_restored = False
        if temp_state and temp_state.get('active') and ctrl.temp_playback_handler:
            logger.info("Detected interrupted temp playback session, attempting recovery...")
            restored = await ctrl.temp_playback_handler.restore(session, temp_state)
            if restored:
                logger.info("Successfully restored temp playback state")
                temp_playback_restored = True
                pending_folder = temp_state.get('folder')
                if pending_folder:
                    ctrl._initialize_file_lock_monitor(pending_folder)
                    if ctrl.file_lock_monitor:
                        ctrl.file_lock_monitor.set_temp_playback_mode(True)
            else:
                logger.warning("Failed to restore temp playback, continuing with normal session resume")
                ctrl.db.clear_temp_playback_state(session['id'])

        if temp_playback_restored:
            # Temp playback owns the pending folder — prevent check_for_rotation
            # from starting new downloads into it while temp playback is active.
            ctrl.download_manager.downloads_triggered_this_rotation = True
            return

        # Normal session resume
        ctrl.download_manager.downloads_triggered_this_rotation = False
        ctrl._just_resumed_session = True

        # Restore prepared playlists from database
        await self._restore_prepared_playlists(session, settings)

        if session.get('stream_title'):
            assert ctrl.stream_manager is not None, "Stream manager not initialized"
            await ctrl.stream_manager.update_title(session['stream_title'])

        # Re-sync the OBS VLC source with the actual live folder contents.
        # After a crash (e.g. network outage) the VLC source may still contain
        # a stale playlist (or even files from the pending folder) that no
        # longer reflects what is actually in the live folder.
        video_folder = settings.get('video_folder', DEFAULT_VIDEO_FOLDER)
        if ctrl.obs_controller:
            success, _ = ctrl.obs_controller.update_vlc_source(
                self._vlc_source, video_folder
            )
            if success:
                logger.info(f"Re-synced VLC source to live folder on resume: {video_folder}")
            else:
                logger.warning("Failed to re-sync VLC source to live folder on resume")

        ctrl._initialize_file_lock_monitor()
        await ctrl._update_category_for_current_video()

        # Restore playback position from crash recovery
        saved_video = session.get('playback_current_video')
        saved_cursor = session.get('playback_cursor_ms', 0)
        if saved_video and saved_cursor and saved_cursor > 0:
            if (ctrl.file_lock_monitor and
                ctrl.file_lock_monitor.current_video_original_name == saved_video):
                ctrl._pending_seek_ms = saved_cursor
                ctrl._pending_seek_video = saved_video
                logger.info(f"Pending resume: {saved_video} at {saved_cursor}ms ({saved_cursor/1000:.1f}s) — waiting for VLC to start")
            else:
                logger.debug(f"Saved video '{saved_video}' no longer current, starting from beginning")

    async def _restore_prepared_playlists(self, session: dict, settings: dict):
        """Restore prepared playlists from database on session resume."""
        ctrl = self._ctrl
        next_playlists = session.get('next_playlists')
        next_playlists_status = session.get('next_playlists_status')

        if not next_playlists:
            return

        try:
            playlist_list = DatabaseManager.parse_json_field(next_playlists, [])
            status_dict: dict = DatabaseManager.parse_json_field(next_playlists_status, {})
            all_completed = all(status_dict.get(pl) == "COMPLETED" for pl in playlist_list)

            if all_completed:
                next_folder = settings.get('next_rotation_folder', DEFAULT_NEXT_ROTATION_FOLDER)
                files_exist = ctrl.db.validate_prepared_playlists_exist(session['id'], next_folder)

                if files_exist:
                    playlist_objects = ctrl.db.get_playlists_with_ids_by_names(playlist_list)
                    if playlist_objects:
                        ctrl.next_prepared_playlists = playlist_objects
                        logger.info(f"Restored prepared playlists from database: {playlist_list}")
                    else:
                        logger.warning(f"Could not fetch playlist objects for: {playlist_list}")
                else:
                    logger.warning(f"Prepared playlist files missing from pending folder, clearing: {playlist_list}")
                    ctrl.db.set_next_playlists(session['id'], [])
            else:
                logger.info(f"Prepared playlists not fully downloaded, auto-resuming downloads now: {status_dict}")
                await ctrl.download_manager.auto_resume_pending_downloads(session['id'], playlist_list)
        except Exception as e:
            logger.error(f"Failed to restore prepared playlists: {e}")
