import logging
import obsws_python as obs
from typing import Optional

logger = logging.getLogger(__name__)


class OBSController:
    """Controller for OBS WebSocket operations."""

    def __init__(self, obs_client: obs.ReqClient):
        self.obs_client = obs_client

    def switch_scene(self, scene_name: str) -> bool:
        """Switch OBS to specified scene."""
        try:
            self.obs_client.set_current_program_scene(scene_name)
            logger.info(f"Switched to scene: {scene_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to switch scene: {e}")
            return False

    def get_current_scene(self) -> Optional[str]:
        """Get the current active scene."""
        try:
            response = self.obs_client.get_current_program_scene()
            return response.current_program_scene_name
        except Exception as e:
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
            logger.error(f"Failed to stop VLC source: {e}")
            return False

    def update_vlc_source(self, source_name: str, video_folder: str) -> bool:
        """
        Update VLC source playlist in OBS.
        Updates the existing source without removing/recreating it.
        """
        try:
            import os
            video_extensions = ('.mp4', '.mkv', '.avi', '.webm', '.flv', '.mov')
            video_files = []

            if os.path.exists(video_folder):
                for filename in sorted(os.listdir(video_folder)):
                    if filename.lower().endswith(video_extensions):
                        full_path = os.path.abspath(os.path.join(video_folder, filename))
                        video_files.append(full_path)

            if not video_files:
                logger.error("No video files found to add to VLC source")
                return False

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
            return True

        except Exception as e:
            logger.error(f"Failed to update VLC source: {e}")
            return False

    def verify_scenes(self, required_scenes: list[str]) -> bool:
        """Verify that required scenes exist in OBS."""
        try:
            scenes = self.obs_client.get_scene_list()
            scene_names = [s['sceneName'] for s in scenes.scenes]

            missing = [scene for scene in required_scenes if scene not in scene_names]

            if missing:
                logger.error(f"Missing scenes in OBS: {', '.join(missing)}")
                return False

            logger.info("All required scenes verified in OBS.")
            return True
        except Exception as e:
            logger.error(f"Failed to verify scenes: {e}")
            return False