"""OBS WebSocket controller for scene and source management.

Provides high-level commands for scene switching, VLC source
management, media playback queries, and connection health checks.
"""
import logging
import time
import obsws_python as obs
from obsws_python.error import OBSSDKRequestError
from typing import Optional
import os
from config.constants import VIDEO_EXTENSIONS

logger = logging.getLogger(__name__)


# Errors that indicate a dead/disconnected OBS WebSocket
_CONNECTION_ERROR_HINTS = (
    'websocket', 'connection', 'socket', 'timed out', 'timeout',
    'winerror', 'forcibly closed', 'expecting value',
)


class OBSController:

    def __init__(self, obs_client: obs.ReqClient):
        self.obs_client = obs_client
        self._is_connected = True

    @property
    def is_connected(self) -> bool:
        """Whether the OBS WebSocket connection is believed to be alive."""
        return self._is_connected

    def _check_connection_error(self, error: Exception) -> None:
        """Mark connection as dead if the error looks like a connectivity failure."""
        msg = str(error).lower()
        if any(hint in msg for hint in _CONNECTION_ERROR_HINTS):
            if self._is_connected:
                logger.warning("OBS connection lost (detected from error)")
            self._is_connected = False

    def switch_scene(self, scene_name: str) -> bool:
        """Switch OBS to specified scene."""
        try:
            self.obs_client.set_current_program_scene(scene_name)
            logger.info(f"Switched to scene: {scene_name}")
            return True
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to switch scene: {e}")
            return False

    def get_current_scene(self) -> Optional[str]:
        """Get the current active scene."""
        try:
            response = self.obs_client.get_current_program_scene()
            return response.current_program_scene_name  # type: ignore
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to get current scene: {e}")
            return None

    def stop_vlc_source(self, source_name: str) -> bool:
        """Stop VLC source playback to release file locks."""
        try:
            # Set the playlist to empty to stop playback and release files
            self.obs_client.set_input_settings(
                name=source_name,
                settings={
                    "playlist": []
                },
                overlay=True  # Only update the playlist field
            )
            logger.info(f"Stopped VLC source: {source_name}")
            return True
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to stop VLC source: {e}")
            return False

    def update_vlc_source(self, source_name: str, video_folder: str, playlist: Optional[list[str]] = None) -> tuple[bool, list[str]]:
        """
        Update VLC source playlist in OBS.
        Updates the existing source without removing/recreating it.
        
        Args:
            source_name: Name of the VLC source in OBS
            video_folder: Path to video folder (for full paths)
            playlist: Optional list of filenames to use instead of scanning folder
                     Useful during temp playback to avoid adding newly downloaded files
        
        Returns:
            Tuple of (success, playlist) where playlist is list of filenames in order
        """
        try:
            video_files = []  # Full paths for OBS
            video_filenames = []  # Just filenames for playlist tracking

            if playlist:
                # Use provided playlist instead of scanning folder
                # This is used in temp playback to maintain consistent playlist
                video_filenames = playlist
                video_files = [os.path.abspath(os.path.join(video_folder, filename)) for filename in playlist]
            elif os.path.exists(video_folder):
                # Scan folder for all video files
                for filename in sorted(os.listdir(video_folder)):
                    if filename.lower().endswith(VIDEO_EXTENSIONS):
                        full_path = os.path.abspath(os.path.join(video_folder, filename))
                        video_files.append(full_path)
                        video_filenames.append(filename)

            if not video_files:
                logger.error("No video files found to add to VLC source")
                return False, []

            self.obs_client.set_input_settings(
                name=source_name,
                settings={
                    "loop": True,
                    "shuffle": False,
                    "playlist": [{"value": path} for path in video_files],
                },
                overlay=False
            )

            logger.info(f"Updated VLC source with {len(video_files)} videos")
            return True, video_filenames

        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to update VLC source: {e}")
            return False, []

    def verify_scenes(self, required_scenes: list[str]) -> bool:
        """Verify that required scenes exist in OBS."""
        try:
            scenes = self.obs_client.get_scene_list()
            scene_names = [s['sceneName'] for s in scenes.scenes]  # type: ignore

            missing = [scene for scene in required_scenes if scene not in scene_names]

            if missing:
                logger.error(f"Missing scenes in OBS: {', '.join(missing)}")
                return False

            logger.info("All required scenes verified in OBS.")
            return True
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to verify scenes: {e}")
            return False

    def ensure_scenes(
        self,
        scene_stream: str,
        scene_pause: str,
        scene_rotation: str,
        vlc_source_name: str,
        video_folder: str,
        pause_image: str,
        rotation_image: str,
    ) -> bool:
        """Ensure all required OBS scenes and sources exist, creating any that are missing.

        Args:
            scene_stream: Name of the main playback scene (e.g. "OSR Stream")
            scene_pause: Name of the pause scene
            scene_rotation: Name of the rotation screen scene
            vlc_source_name: Name of the VLC video source inside the stream scene
            video_folder: Path to the live video folder for VLC source
            pause_image: Absolute path to the default pause image
            rotation_image: Absolute path to the default rotation image

        Returns:
            True if all scenes/sources are ready, False on fatal error.
        """
        try:
            scenes_response = self.obs_client.get_scene_list()
            existing_scenes = {s['sceneName'] for s in scenes_response.scenes}  # type: ignore
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to query OBS scenes: {e}")
            return False

        # Get canvas resolution for fullscreen transforms
        canvas_width, canvas_height = self._get_canvas_size()

        # --- Stream scene ---
        if scene_stream not in existing_scenes:
            if not self._create_scene(scene_stream):
                return False
            self._add_vlc_source(scene_stream, vlc_source_name, video_folder, canvas_width, canvas_height)
        else:
            # Scene exists — make sure VLC source is present
            if not self._scene_has_input(scene_stream, vlc_source_name):
                self._add_vlc_source(scene_stream, vlc_source_name, video_folder, canvas_width, canvas_height)

        # --- Pause scene ---
        if scene_pause not in existing_scenes:
            if not self._create_scene(scene_pause):
                return False
            self._add_image_source(scene_pause, f"{scene_pause} Image", pause_image, canvas_width, canvas_height)
        
        # --- Rotation scene ---
        if scene_rotation not in existing_scenes:
            if not self._create_scene(scene_rotation):
                return False
            self._add_image_source(scene_rotation, f"{scene_rotation} Image", rotation_image, canvas_width, canvas_height)
        
        logger.info("All required OBS scenes and sources are ready")
        return True

    # ------------------------------------------------------------------
    # Private helpers for scene / source creation
    # ------------------------------------------------------------------

    def _get_canvas_size(self) -> tuple[int, int]:
        """Return the OBS base (canvas) resolution as (width, height)."""
        try:
            video_settings = self.obs_client.get_video_settings()
            return video_settings.base_width, video_settings.base_height  # type: ignore
        except Exception as e:
            logger.warning(f"Could not get OBS canvas size, defaulting to 1920x1080: {e}")
            return 1920, 1080

    def _create_scene(self, scene_name: str) -> bool:
        """Create a new OBS scene."""
        try:
            self.obs_client.create_scene(scene_name)
            logger.info(f"Created OBS scene: {scene_name}")
            return True
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to create scene '{scene_name}': {e}")
            return False

    def _scene_has_input(self, scene_name: str, input_name: str) -> bool:
        """Check whether a scene already contains a specific input source."""
        try:
            items = self.obs_client.get_scene_item_list(scene_name)
            for item in items.scene_items:  # type: ignore
                if item.get('sourceName') == input_name:
                    return True
            return False
        except Exception as e:
            logger.debug(f"Could not enumerate items in scene '{scene_name}': {e}")
            return False

    def _add_vlc_source(
        self, scene_name: str, source_name: str, video_folder: str,
        canvas_width: int, canvas_height: int,
    ) -> None:
        """Add a VLC Video Source to a scene and set it to fill the canvas."""
        try:
            # Build an initial playlist from the video folder
            playlist_entries: list[dict] = []
            if os.path.exists(video_folder):
                for fn in sorted(os.listdir(video_folder)):
                    if fn.lower().endswith(VIDEO_EXTENSIONS):
                        playlist_entries.append({"value": os.path.abspath(os.path.join(video_folder, fn))})

            try:
                self.obs_client.create_input(
                    sceneName=scene_name,
                    inputName=source_name,
                    inputKind="vlc_source",
                    inputSettings={
                        "loop": True,
                        "shuffle": False,
                        "playlist": playlist_entries,
                    },
                    sceneItemEnabled=True,
                )
            except OBSSDKRequestError as req_err:
                if req_err.code == 601:
                    # Input already exists globally — add it to this scene
                    logger.info(f"VLC source '{source_name}' already exists, adding to scene '{scene_name}'")
                    self.obs_client.create_scene_item(scene_name, source_name, enabled=True)
                else:
                    raise
            logger.info(f"Added VLC source '{source_name}' to scene '{scene_name}'")

            # Stretch to fill canvas
            self._set_source_fullscreen(scene_name, source_name, canvas_width, canvas_height)
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to add VLC source to '{scene_name}': {e}")

    def _add_image_source(
        self, scene_name: str, source_name: str, image_path: str,
        canvas_width: int, canvas_height: int,
    ) -> None:
        """Add an Image source to a scene and set it to fill the canvas."""
        if not os.path.exists(image_path):
            logger.warning(
                f"Default image not found at {image_path} — scene '{scene_name}' created "
                f"without a source. Drop an image there and restart, or add a source manually."
            )
            return
        try:
            try:
                self.obs_client.create_input(
                    sceneName=scene_name,
                    inputName=source_name,
                    inputKind="image_source",
                    inputSettings={"file": os.path.abspath(image_path)},
                    sceneItemEnabled=True,
                )
            except OBSSDKRequestError as req_err:
                if req_err.code == 601:
                    # Input already exists globally — add it to this scene
                    logger.info(f"Image source '{source_name}' already exists, adding to scene '{scene_name}'")
                    self.obs_client.create_scene_item(scene_name, source_name, enabled=True)
                else:
                    raise
            logger.info(f"Added image source '{source_name}' to scene '{scene_name}'")

            self._set_source_fullscreen(scene_name, source_name, canvas_width, canvas_height)
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to add image source to '{scene_name}': {e}")

    def _set_source_fullscreen(
        self, scene_name: str, source_name: str,
        canvas_width: int, canvas_height: int,
    ) -> None:
        """Set a scene item's transform to fill the entire canvas."""
        try:
            # Find the scene item ID
            items = self.obs_client.get_scene_item_list(scene_name)
            item_id = None
            for item in items.scene_items:  # type: ignore
                if item.get('sourceName') == source_name:
                    item_id = item.get('sceneItemId')
                    break
            if item_id is None:
                logger.warning(f"Could not find '{source_name}' in scene '{scene_name}' to set transform")
                return

            self.obs_client.set_scene_item_transform(
                scene_name=scene_name,
                item_id=item_id,
                transform={
                    "boundsType": "OBS_BOUNDS_STRETCH",
                    "boundsWidth": float(canvas_width),
                    "boundsHeight": float(canvas_height),
                    "positionX": 0.0,
                    "positionY": 0.0,
                },
            )
            logger.debug(f"Set '{source_name}' in '{scene_name}' to {canvas_width}x{canvas_height} fullscreen")
        except Exception as e:
            logger.warning(f"Failed to set fullscreen transform for '{source_name}': {e}")

    def get_media_input_status(self, source_name: str) -> Optional[dict]:
        """Get playback status of a media input source (VLC).
        
        Returns dict with:
        - media_state: Current state (PLAYING, PAUSED, STOPPED, ENDED, etc.)
        - media_cursor: Current playback position in milliseconds
        - media_duration: Total duration in milliseconds
        
        Returns None if source not found or error occurs.
        """
        try:
            response = self.obs_client.get_media_input_status(name=source_name)
            return {
                'media_state': response.media_state,  # type: ignore
                'media_cursor': response.media_cursor,  # type: ignore (milliseconds)
                'media_duration': response.media_duration,  # type: ignore (milliseconds)
            }
        except Exception as e:
            self._check_connection_error(e)
            logger.debug(f"Failed to get media input status for {source_name}: {e}")
            return None

    def seek_media(self, source_name: str, position_ms: int) -> bool:
        """Seek VLC media source to a specific position.
        
        Args:
            source_name: Name of the VLC source
            position_ms: Position to seek to in milliseconds
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.obs_client.set_media_input_cursor(
                name=source_name,
                cursor=position_ms
            )
            logger.info(f"Seeked {source_name} to {position_ms}ms ({position_ms/1000:.1f}s)")
            return True
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to seek media {source_name}: {e}")
            return False

    def play_media(self, source_name: str) -> bool:
        """Trigger play action on a media input source.
        
        Args:
            source_name: Name of the media input source (VLC)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.obs_client.trigger_media_input_action(
                name=source_name,
                action="OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PLAY"
            )
            logger.info(f"Triggered play on {source_name}")
            return True
        except Exception as e:
            self._check_connection_error(e)
            logger.debug(f"Failed to trigger play on {source_name}: {e}")
            return False

    def switch_scene_and_wait(self, scene_name: str, wait_seconds: float = 1.0) -> bool:
        """Switch scene and wait for transition.
        
        Args:
            scene_name: Name of scene to switch to
            wait_seconds: Seconds to wait after switching
        
        Returns:
            True if successful
        """
        success = self.switch_scene(scene_name)
        if success:
            time.sleep(wait_seconds)
        return success

    def prepare_for_content_switch(self, scene_rotation_screen: str, 
                                   vlc_source_name: str, wait_seconds: float = 3.0) -> bool:
        """Prepare for content switch (switch to Rotation screen scene and stop VLC).
        
        Args:
            scene_rotation_screen: Name of Rotation screen scene
            vlc_source_name: Name of VLC source
            wait_seconds: Seconds to wait for OS to release file locks
        
        Returns:
            True if successful
        """
        # Switch to Rotation screen scene
        if not self.switch_scene(scene_rotation_screen):
            return False
        
        # Stop VLC source
        if not self.stop_vlc_source(vlc_source_name):
            return False
        
        # Wait for file locks to release
        time.sleep(wait_seconds)
        return True

    def finalize_content_switch(self, vlc_source_name: str, video_folder: str,
                                target_scene: str) -> bool:
        """Finalize content switch (update VLC source and switch to target scene).
        
        Args:
            vlc_source_name: Name of VLC source to update
            video_folder: Folder containing new video content
            target_scene: Scene to switch to after update
        
        Returns:
            True if successful
        """
        # Update VLC source with new content
        success, _ = self.update_vlc_source(vlc_source_name, video_folder)
        if not success:
            logger.error("Failed to update VLC source during finalization")
            return False
        
        # Switch to target scene
        if not self.switch_scene(target_scene):
            logger.error("Failed to switch to target scene during finalization")
            return False
        
        return True

    def get_playback_position_ms(self, source_name: str) -> int:
        """Get current playback position of media source.
        
        Args:
            source_name: Name of media source
        
        Returns:
            Playback position in milliseconds, or 0 if unable to determine
        """
        status = self.get_media_input_status(source_name)
        if not status:
            return 0
        
        position = status.get('media_cursor')
        return position if position is not None else 0

    def get_total_media_duration_ms(self, source_name: str) -> int:
        """Get total duration of media source.
        
        Args:
            source_name: Name of media source
        
        Returns:
            Total duration in milliseconds, or 0 if unable to determine
        """
        status = self.get_media_input_status(source_name)
        if not status:
            return 0
        
        duration = status.get('media_duration')
        return duration if duration is not None else 0

    def get_media_state(self, source_name: str) -> Optional[str]:
        """Get current playback state of media source.
        
        Args:
            source_name: Name of media source
        
        Returns:
            Media state string (PLAYING, PAUSED, STOPPED, ENDED, etc.) or None
        """
        status = self.get_media_input_status(source_name)
        if not status:
            return None
        
        return status.get('media_state')

    # ── Alert text overlay ───────────────────────────────────────────

    def ensure_alert_text_source(self, scene_name: str, source_name: str) -> bool:
        """Create a hidden text (GDI+) source on the stream scene for fallback alerts.

        The source is created once and left hidden.  ``show_alert_text`` /
        ``hide_alert_text`` toggle its scene-item visibility so there is no
        flicker from repeatedly creating / destroying inputs.
        """
        if self._scene_has_input(scene_name, source_name):
            self._set_scene_item_enabled(scene_name, source_name, False)
            logger.debug(f"Alert text source '{source_name}' already exists in '{scene_name}'")
            return True

        try:
            canvas_width, canvas_height = self._get_canvas_size()
            font_size = max(36, canvas_height // 20)

            self.obs_client.create_input(
                sceneName=scene_name,
                inputName=source_name,
                inputKind="text_gdiplus_v2",
                inputSettings={
                    "text": "",
                    "font": {"face": "Arial", "size": font_size, "style": "Bold"},
                    "color": 0xFFFFFFFF,
                    "bk_color": 0xCC000000,
                    "bk_opacity": 80,
                    "outline": True,
                    "outline_color": 0xFF000000,
                    "outline_size": 3,
                    "align": "center",
                    "valign": "center",
                    "extents": True,
                    "extents_cx": canvas_width,
                    "extents_cy": font_size * 3,
                    "extents_wrap": True,
                },
                sceneItemEnabled=False,
            )
            self._position_alert_source(scene_name, source_name, canvas_width, canvas_height, font_size)
            logger.info(f"Created alert text source '{source_name}' in '{scene_name}' (hidden)")
            return True
        except OBSSDKRequestError as req_err:
            if req_err.code == 601:
                try:
                    self.obs_client.create_scene_item(scene_name, source_name, enabled=False)
                    canvas_width, canvas_height = self._get_canvas_size()
                    font_size = max(36, canvas_height // 20)
                    self._position_alert_source(scene_name, source_name, canvas_width, canvas_height, font_size)
                    logger.info(f"Added existing alert source '{source_name}' to '{scene_name}' (hidden)")
                    return True
                except Exception as e2:
                    logger.error(f"Failed to add existing alert source to scene: {e2}")
                    return False
            logger.error(f"OBS error creating alert source: {req_err}")
            return False
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to create alert text source: {e}")
            return False

    def _position_alert_source(self, scene_name: str, source_name: str,
                               canvas_width: int, canvas_height: int,
                               font_size: int) -> None:
        """Position the alert text at the bottom of the canvas."""
        try:
            items = self.obs_client.get_scene_item_list(scene_name)
            item_id = None
            for item in items.scene_items:  # type: ignore
                if item.get('sourceName') == source_name:
                    item_id = item.get('sceneItemId')
                    break
            if item_id is None:
                return
            text_height = float(font_size * 3)
            self.obs_client.set_scene_item_transform(
                scene_name=scene_name,
                item_id=item_id,
                transform={
                    "positionX": 0.0,
                    "positionY": float(canvas_height) - text_height - 20.0,
                    "boundsType": "OBS_BOUNDS_NONE",
                },
            )
        except Exception as e:
            logger.warning(f"Failed to position alert source: {e}")

    def set_alert_text(self, source_name: str, text: str) -> bool:
        """Update the alert text content."""
        try:
            self.obs_client.set_input_settings(
                name=source_name,
                settings={"text": text},
                overlay=True,
            )
            return True
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to set alert text: {e}")
            return False

    def show_alert_text(self, scene_name: str, source_name: str, text: str) -> bool:
        """Set text and make the alert source visible."""
        self.set_alert_text(source_name, text)
        return self._set_scene_item_enabled(scene_name, source_name, True)

    def hide_alert_text(self, scene_name: str, source_name: str) -> bool:
        """Hide the alert text source."""
        return self._set_scene_item_enabled(scene_name, source_name, False)

    def _set_scene_item_enabled(self, scene_name: str, source_name: str, enabled: bool) -> bool:
        """Toggle visibility of a scene item."""
        try:
            items = self.obs_client.get_scene_item_list(scene_name)
            for item in items.scene_items:  # type: ignore
                if item.get('sourceName') == source_name:
                    item_id = item.get('sceneItemId')
                    self.obs_client.set_scene_item_enabled(
                        scene_name=scene_name,
                        item_id=item_id,
                        enabled=enabled,
                    )
                    logger.debug(f"{'Showed' if enabled else 'Hid'} '{source_name}' in '{scene_name}'")
                    return True
            logger.warning(f"Source '{source_name}' not found in scene '{scene_name}'")
            return False
        except Exception as e:
            self._check_connection_error(e)
            logger.error(f"Failed to toggle scene item visibility: {e}")
            return False