"""
Playback Monitor — Event-driven video transition detection via OBS WebSocket.

Detects video transitions via OBS ``MediaInputPlaybackEnded`` /
``MediaInputPlaybackStarted`` events fed into a thread-safe queue by the
``OBSConnectionManager``'s EventClient.

The monitor still owns:
* current-video tracking (filename, index, original name)
* video deletion on transition
* VLC source refresh after deletion
* ``all_content_consumed`` / ``needs_vlc_refresh`` signalling
* temp-playback and prepared-rotation modes
* suspend / resume during OBS freeze recovery
* per-video category resolution
"""

import logging
import os
from queue import Queue, Empty
from typing import Optional, TYPE_CHECKING

from config.constants import VIDEO_EXTENSIONS
from utils.video_utils import strip_ordering_prefix, resolve_category_for_video

if TYPE_CHECKING:
    from controllers.obs_controller import OBSController
    from core.database import DatabaseManager
    from config.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class PlaybackMonitor:
    """Tracks OBS VLC source playback using WebSocket media events."""

    def __init__(
        self,
        db: 'DatabaseManager',
        obs_controller: 'OBSController',
        vlc_source_name: str,
        event_queue: Queue,
        config: Optional['ConfigManager'] = None,
        scene_stream: str = "OSR Stream",
    ):
        self.db = db
        self.obs_controller = obs_controller
        self.vlc_source_name = vlc_source_name
        self.config = config
        self.scene_stream = scene_stream

        # Thread-safe queue fed by OBSConnectionManager EventClient
        self._event_queue: Queue = event_queue

        self.video_folder: str = ""
        self._current_video: Optional[str] = None
        self._all_content_consumed: bool = False
        self._temp_playback_mode: bool = False
        self._delete_on_transition: bool = True
        self._needs_vlc_refresh: bool = False

        # Suspend flag — check() becomes a no-op while True.
        self._suspended: bool = False

        # Suppression counter — absorbs spurious "started" events fired by
        # VLC when the source is initialised or reconfigured (update_vlc_source).
        # Each VLC reconfiguration fires exactly one "started" event that does
        # NOT correspond to a real track change.
        self._suppress_started: int = 0

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def initialize(self, video_folder: str) -> None:
        """Prepare the monitor for a new rotation.

        Scans *video_folder* for video files and sets the first file as
        the current video.  Also drains any stale events left over from
        a previous rotation.
        """
        self.video_folder = video_folder
        self._current_video = None
        self._all_content_consumed = False
        self._needs_vlc_refresh = False
        self._temp_playback_mode = False
        self._delete_on_transition = True

        # Drain stale events from previous rotation / OBS reconnect
        self._drain_queue()
        # VLC fires a "started" event when it loads the new playlist —
        # suppress it so it isn't mistaken for a video transition.
        self._suppress_started = 1

        files = self._get_video_files()
        if not files:
            logger.warning("Playback monitor initialized with empty folder")
            logger.info(f"Playback monitor tracking 0 videos in {video_folder}")
            return

        self._current_video = files[0]
        self._suspended = False
        logger.info(
            f"Playback monitor initialized: current video = {files[0]}"
        )
        logger.info(
            f"Playback monitor tracking {len(files)} videos in {video_folder}"
        )

    def reset(self) -> None:
        """Reset all internal state."""
        self._current_video = None
        self._all_content_consumed = False
        self._needs_vlc_refresh = False
        self._suspended = False
        self._drain_queue()
        logger.debug("Playback monitor reset")

    # ------------------------------------------------------------------
    # Suspend / resume (OBS freeze recovery)
    # ------------------------------------------------------------------

    def suspend(self) -> None:
        """Suspend monitoring — ``check()`` becomes a no-op."""
        self._suspended = True
        self._drain_queue()
        logger.info("Playback monitor suspended")

    def resume(self) -> None:
        """Resume monitoring after a suspension."""
        self._suspended = False
        self._drain_queue()
        # OBS reconnect / freeze recovery may fire a spurious started event
        self._suppress_started = 1
        logger.info("Playback monitor resumed")

    # ------------------------------------------------------------------
    # Mode toggles
    # ------------------------------------------------------------------

    def set_temp_playback_mode(self, enabled: bool) -> None:
        self._temp_playback_mode = enabled
        self._drain_queue()
        logger.info(
            f"Playback monitor temp playback mode: "
            f"{'enabled' if enabled else 'disabled'}"
        )

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def all_content_consumed(self) -> bool:
        return self._all_content_consumed

    @property
    def needs_vlc_refresh(self) -> bool:
        return self._needs_vlc_refresh

    def clear_vlc_refresh_flag(self) -> None:
        self._needs_vlc_refresh = False

    @property
    def current_video(self) -> Optional[str]:
        """Filename with ordering prefix (e.g. ``01_MW2_…``)."""
        return self._current_video

    @property
    def current_video_original_name(self) -> Optional[str]:
        """Filename with ordering prefix stripped — use for DB lookups."""
        if self._current_video:
            return strip_ordering_prefix(self._current_video)
        return None

    # ------------------------------------------------------------------
    # Core: check for transitions
    # ------------------------------------------------------------------

    def check(self) -> dict:
        """Drain the event queue and process any video transitions.

        Called every ~1 s from the main loop.

        Returns a dict:
        ``{ transition, previous_video, current_video, all_consumed }``
        """
        result: dict = {
            'transition': False,
            'previous_video': None,
            'current_video': (
                strip_ordering_prefix(self._current_video)
                if self._current_video else None
            ),
            'all_consumed': False,
        }

        if not self.video_folder or self._all_content_consumed:
            result['all_consumed'] = self._all_content_consumed
            return result

        if self._suspended:
            return result

        # Skip when OBS is disconnected — drain stale events so they
        # don't pile up and trigger a burst of false transitions later.
        if self.obs_controller and not self.obs_controller.is_connected:
            self._drain_queue()
            return result

        # Skip when not on the stream scene — drain events for the same
        # reason (VLC may keep firing while on the pause/rotation screen).
        if self.obs_controller:
            current_scene = self.obs_controller.get_current_scene()
            if current_scene and current_scene != self.scene_stream:
                self._drain_queue()
                return result

        if not self._current_video:
            files = self._get_video_files()
            if not files:
                self._all_content_consumed = True
                result['all_consumed'] = True
                return result
            self._current_video = files[0]
            result['current_video'] = strip_ordering_prefix(self._current_video)
            return result

        # ── Process events from the OBS EventClient ──────────────
        transition_count = self._count_transitions()

        if transition_count == 0:
            return result

        # Process each transition sequentially (handles rapid skips)
        for _ in range(transition_count):
            if self._all_content_consumed:
                break

            previous_video = self._current_video
            previous_original = strip_ordering_prefix(previous_video)
            files = self._get_video_files()
            is_last = len(files) <= 1

            if is_last:
                # Last video finished — check temp playback refresh
                if self._temp_playback_mode:
                    logger.info(
                        "Last video done in temp playback — signaling VLC refresh"
                    )
                    self._needs_vlc_refresh = True
                    result['transition'] = True
                    result['previous_video'] = previous_original
                    result['current_video'] = None
                    return result

                # Normal mode: delete last video, mark consumed
                if self._delete_on_transition:
                    filepath = os.path.join(self.video_folder, previous_video)
                    self._delete_video(filepath)

                self._all_content_consumed = True
                self._current_video = None
                result['transition'] = True
                result['previous_video'] = previous_original
                result['current_video'] = None
                result['all_consumed'] = True
                logger.info(
                    f"Final video finished: {previous_original} — "
                    f"all content consumed"
                )
                return result

            # Normal mid-playlist transition
            if self._delete_on_transition:
                filepath = os.path.join(self.video_folder, previous_video)
                deleted = self._delete_video(filepath)
                if not deleted:
                    # File still locked — skip this transition and retry
                    # on the next tick.  Don't update VLC or advance the
                    # pointer; VLC is still playing this file anyway.
                    logger.warning(
                        f"Skipping transition for {previous_video} — "
                        f"file could not be deleted, will retry next cycle"
                    )
                    break
                self._update_vlc_source()

            # Advance to next video
            files = self._get_video_files()
            if self._delete_on_transition:
                # After deletion the next file is always first
                self._current_video = files[0] if files else None
            else:
                # Prepared-rotation mode — advance by index
                cur_idx = (
                    files.index(previous_video)
                    if previous_video in files else -1
                )
                if cur_idx >= 0 and cur_idx + 1 < len(files):
                    self._current_video = files[cur_idx + 1]
                else:
                    self._current_video = None

            if self._current_video:
                current_original = strip_ordering_prefix(self._current_video)
                logger.info(
                    f"Video transition: {previous_original} -> {current_original}"
                )
                # Only populate the *last* transition into the result
                result['transition'] = True
                result['previous_video'] = previous_original
                result['current_video'] = current_original
            else:
                self._all_content_consumed = True
                result['transition'] = True
                result['previous_video'] = previous_original
                result['current_video'] = None
                result['all_consumed'] = True
                logger.info(
                    f"Final video finished: {previous_original} — "
                    f"all content consumed"
                )
                return result

        return result

    # ------------------------------------------------------------------
    # Event queue helpers
    # ------------------------------------------------------------------

    def _drain_queue(self) -> None:
        """Discard all pending events."""
        while True:
            try:
                self._event_queue.get_nowait()
            except Empty:
                break

    def _count_transitions(self) -> int:
        """Read events from the queue and return how many transitions occurred.

        OBS VLC source event behaviour:

        * ``MediaInputPlaybackStarted`` fires each time a new **track**
          begins — both on natural track advances and when the source is
          reconfigured (``update_vlc_source`` / ``initialize``).
        * ``MediaInputPlaybackEnded`` fires when the **entire playlist**
          finishes (last track only), *not* per-track.

        Because ``started`` is the per-track signal we rely on it for
        mid-playlist transitions.  However, VLC also fires ``started``
        when the source is initialised or reconfigured, so we maintain a
        ``_suppress_started`` counter that absorbs those spurious events.

        An ``ended`` event always counts as a transition (it means the
        last track finished).  Each ``ended`` also locally suppresses one
        following ``started`` so that an ``ended→started`` pair counts as
        one transition, not two.
        """
        events: list[str] = []
        while True:
            try:
                events.append(self._event_queue.get_nowait())
            except Empty:
                break

        if not events:
            return 0

        transitions = 0
        # Each "ended" absorbs the immediately following "started" so an
        # ended→started pair produced by OBS counts as 1 transition.
        local_suppress = 0

        for evt in events:
            if evt == "ended":
                transitions += 1
                local_suppress += 1
            elif evt == "started":
                # 1. Spurious event from VLC init / source update
                if self._suppress_started > 0:
                    self._suppress_started -= 1
                    logger.debug(
                        "Suppressed spurious 'started' event "
                        f"(VLC init/update, remaining: {self._suppress_started})"
                    )
                # 2. Paired with a preceding "ended" (same transition)
                elif local_suppress > 0:
                    local_suppress -= 1
                # 3. Genuine per-track advance (no preceding "ended")
                else:
                    transitions += 1

        if transitions > 0:
            logger.debug(
                f"Processed {len(events)} media events → "
                f"{transitions} transition(s)"
            )

        return transitions

    # ------------------------------------------------------------------
    # File helpers
    # ------------------------------------------------------------------

    def _get_video_files(self) -> list[str]:
        """Video files in folder, sorted alphabetically (prefix-ordered)."""
        if not self.video_folder or not os.path.exists(self.video_folder):
            return []
        try:
            return sorted(
                f for f in os.listdir(self.video_folder)
                if f.lower().endswith(VIDEO_EXTENSIONS)
            )
        except Exception as e:
            logger.warning(
                f"Failed to list video files in {self.video_folder}: {e}"
            )
            return []

    def _delete_video(self, filepath: str) -> bool:
        """Delete a completed video file."""
        try:
            filename = os.path.basename(filepath)
            os.remove(filepath)
            logger.info(f"Deleted completed video: {filename}")
            return True
        except PermissionError:
            filename = os.path.basename(filepath)
            logger.warning(
                f"Cannot delete {filename} — file still locked, "
                f"will retry next cycle"
            )
            return False
        except Exception as e:
            filename = os.path.basename(filepath)
            logger.error(f"Failed to delete video {filename}: {e}")
            return False

    def _update_vlc_source(self) -> None:
        """Push current folder contents to the OBS VLC source.

        After reconfiguration VLC fires a spurious ``started`` event for
        the new first track.  We increment ``_suppress_started`` so that
        ``_count_transitions`` ignores it.
        """
        if not self.obs_controller:
            return
        try:
            success, _ = self.obs_controller.update_vlc_source(
                self.vlc_source_name, self.video_folder
            )
            if success:
                # VLC will fire a "started" event from the reconfiguration —
                # mark it for suppression.
                self._suppress_started += 1
                files = self._get_video_files()
                logger.debug(f"Updated VLC source: {len(files)} videos remaining")
        except Exception as e:
            logger.error(f"Failed to update VLC source after deletion: {e}")

    # ------------------------------------------------------------------
    # Category resolution
    # ------------------------------------------------------------------

    def get_category_for_current_video(self) -> Optional[dict[str, str]]:
        """Per-platform stream categories for the current video."""
        if not self._current_video or not self.config:
            return None
        return resolve_category_for_video(
            self._current_video, self.db, self.config
        )
