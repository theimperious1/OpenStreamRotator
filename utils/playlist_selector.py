import logging
import json
from typing import List, Dict, Optional
from core.database import DatabaseManager
from config.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class PlaylistSelector:
    """Intelligently selects playlists for rotation."""

    def __init__(self, db: DatabaseManager, config: ConfigManager):
        """
        Initialize playlist selector.
        
        Args:
            db: DatabaseManager instance
            config: ConfigManager instance
        """
        self.db = db
        self.config = config

    def select_for_rotation(self, manual_selection: Optional[List[str]] = None) -> List[Dict]:
        """
        Select playlists for the next rotation.
        
        Uses manual selection if provided, otherwise automatic selection.
        Only selects playlists that are defined in the config file.
        Excludes playlists currently in preparation (next_playlists).
        
        Args:
            manual_selection: Optional list of playlist names to manually select
        
        Returns:
            List of playlist dictionaries to play
        """
        # Get allowed playlist names from config
        config_playlists = self.config.get_playlists()
        allowed_names = {p['name'] for p in config_playlists if p.get('enabled', True)}
        
        # Get playlists that are currently being prepared/downloaded
        excluded_playlist_names = self._get_playlists_in_pipeline()
        
        if manual_selection:
            return self._select_manual(manual_selection, allowed_names, excluded_playlist_names)
        else:
            return self._select_automatic(allowed_names, excluded_playlist_names)

    def _get_playlists_in_pipeline(self) -> set:
        """
        Get playlists that are currently in the pipeline and actually being downloaded.
        
        Only returns playlists with COMPLETED status (ready to use).
        Ignores PENDING playlists as they may be incomplete/corrupted from failed downloads.
        
        Returns:
            Set of playlist names currently being prepared (COMPLETED status only)
        """
        try:
            session = self.db.get_current_session()
            if not session:
                return set()
            
            # Get next_playlists and their status from current session
            next_playlists_json = session.get('next_playlists')
            next_playlists_status_json = session.get('next_playlists_status')
            
            if not next_playlists_json:
                return set()
            
            next_playlist_names = json.loads(next_playlists_json) if isinstance(next_playlists_json, str) else next_playlists_json
            status_dict = json.loads(next_playlists_status_json) if isinstance(next_playlists_status_json, str) else (next_playlists_status_json or {})
            
            # Only exclude playlists that are COMPLETED (ready to use next rotation)
            # PENDING playlists may be incomplete/corrupted and should be re-downloaded
            completed_playlists = [pl for pl in next_playlist_names if status_dict.get(pl) == "COMPLETED"]
            
            if completed_playlists:
                logger.debug(f"Excluding from selection (prepared and ready): {completed_playlists}")
            
            return set(completed_playlists)
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            logger.debug(f"Could not parse in-pipeline playlists: {e}")
        
        return set()

    def _select_manual(self, manual_selection: List[str], allowed_names: set, excluded_names: Optional[set] = None) -> List[Dict]:
        """
        Manually select specific playlists.
        
        Args:
            manual_selection: List of playlist names to select
            allowed_names: Set of playlist names allowed by config
            excluded_names: Set of playlist names to exclude (currently preparing)
        
        Returns:
            List of selected playlists
        """
        if excluded_names is None:
            excluded_names = set()
        
        all_playlists = self.db.get_enabled_playlists()
        selected = [p for p in all_playlists 
                   if p['name'] in manual_selection 
                   and p['name'] in allowed_names
                   and p['name'] not in excluded_names]
        logger.info(f"Manual selection: {[p['name'] for p in selected]}")
        return selected

    def _select_automatic(self, allowed_names: set, excluded_names: Optional[set] = None) -> List[Dict]:
        """
        Automatically select playlists based on rotation strategy.
        Ensures at least 1-2 long (non-shorts) playlists are always included to prevent
        scenarios where all shorts are selected (which would cause massive download times).
        Excludes playlists currently being prepared.
        
        Args:
            allowed_names: Set of playlist names allowed by config
            excluded_names: Set of playlist names to exclude (currently preparing)
        
        Returns:
            List of selected playlists balanced with long and shorts content
        """
        if excluded_names is None:
            excluded_names = set()
        
        settings = self.config.get_settings()
        min_playlists = settings.get('min_playlists_per_rotation', 2)
        max_playlists = settings.get('max_playlists_per_rotation', 4)

        all_playlists = self.db.get_enabled_playlists()
        
        # Create a lookup map of is_short flag from config
        config_playlists = self.config.get_playlists()
        is_short_map = {p['name']: p.get('is_short', False) for p in config_playlists}
        
        # Filter to only include playlists in config AND not currently preparing
        all_playlists = [p for p in all_playlists 
                        if p['name'] in allowed_names 
                        and p['name'] not in excluded_names]

        if len(all_playlists) == 0:
            logger.error("No eligible playlists available! (all in preparation or disabled)")
            return []

        # Separate playlists into long-form and shorts (using is_short flag from config)
        long_playlists = [p for p in all_playlists if not is_short_map.get(p['name'], False)]
        shorts_playlists = [p for p in all_playlists if is_short_map.get(p['name'], False)]
        
        logger.debug(f"Available long playlists: {[p['name'] for p in long_playlists]}")
        logger.debug(f"Available shorts playlists: {[p['name'] for p in shorts_playlists]}")

        # Determine number of playlists to select
        num_to_select = min(len(all_playlists), max_playlists)
        num_to_select = max(num_to_select, min_playlists)
        
        # Ensure at least 1-2 long playlists are included (don't let all shorts be selected)
        # Use min_playlists as the minimum number of long playlists to include
        min_long_playlists = max(1, min_playlists - 1) if min_playlists > 1 else 1
        
        # If we have fewer long playlists than required, use what we have
        num_long_to_select = min(len(long_playlists), min_long_playlists)
        
        if num_long_to_select == 0 and len(long_playlists) > 0:
            # Edge case: ensure at least 1 long playlist if any exist
            num_long_to_select = 1
        
        # Calculate how many shorts we can add
        num_shorts_to_select = num_to_select - num_long_to_select
        num_shorts_to_select = min(num_shorts_to_select, len(shorts_playlists))
        
        # If we don't have enough shorts, add more long playlists to reach minimum
        if num_shorts_to_select == 0 and num_long_to_select < num_to_select:
            # No shorts available, use more long playlists
            num_long_to_select = min(len(long_playlists), num_to_select)
        
        # Select from each group (sorted by last_played and priority)
        selected_long = long_playlists[:num_long_to_select]
        selected_shorts = shorts_playlists[:num_shorts_to_select]
        selected = selected_long + selected_shorts

        logger.info(f"Auto-selected {len(selected)} playlists: {[p['name'] for p in selected]} "
                   f"({len(selected_long)} long, {len(selected_shorts)} shorts) "
                   f"(excluded from preparation: {excluded_names})")
        return selected
