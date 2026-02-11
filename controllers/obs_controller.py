import logging
import time
import obsws_python as obs
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
    """Controller for OBS WebSocket operations."""

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

    def prepare_for_content_switch(self, scene_content_switch: str, 
                                   vlc_source_name: str, wait_seconds: float = 3.0) -> bool:
        """Prepare for content switch (switch to content-switch scene and stop VLC).
        
        Args:
            scene_content_switch: Name of content-switch scene
            vlc_source_name: Name of VLC source
            wait_seconds: Seconds to wait for OS to release file locks
        
        Returns:
            True if successful
        """
        # Switch to content-switch scene
        if not self.switch_scene(scene_content_switch):
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