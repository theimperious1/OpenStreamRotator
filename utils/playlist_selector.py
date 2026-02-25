"""Playlist selector for rotation scheduling.

Selects playlists for each rotation using priority weights,
play history, and exclusion rules to avoid repetition.
"""
import logging
import json
from typing import List, Dict, Optional
from core.database import DatabaseManager
from config.config_manager import ConfigManager

from config.constants import DEFAULT_MIN_PLAYLISTS, DEFAULT_MAX_PLAYLISTS

logger = logging.getLogger(__name__)


class PlaylistSelector:

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
        Get playlists that are currently in the pipeline (currently playing or preparing/downloading).
        
        Returns:
            Set of playlist names currently being played or prepared
        """
        try:
            session = self.db.get_current_session()
            if not session:
                return set()
            
            excluded = set()
            
            # Exclude playlists_selected (currently playing in live/ folder)
            playlists_selected_json = session.get('playlists_selected')
            if playlists_selected_json:
                try:
                    playlists_selected_ids = json.loads(playlists_selected_json) if isinstance(playlists_selected_json, str) else playlists_selected_json
                    for playlist_id in playlists_selected_ids:
                        playlist = self.db.get_playlist(playlist_id)
                        if playlist:
                            excluded.add(playlist['name'])
                except (json.JSONDecodeError, TypeError, KeyError):
                    pass
            
            # Get next_playlists from current session (what's being prepared)
            next_playlists_json = session.get('next_playlists')
            if next_playlists_json:
                next_playlist_names = self.db.parse_json_field(next_playlists_json, [])
                excluded.update(next_playlist_names)
            
            if excluded:
                logger.debug(f"Excluding from selection (currently playing or preparing): {excluded}")
            
            return excluded
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
        Excludes playlists currently being prepared.
        
        Args:
            allowed_names: Set of playlist names allowed by config
            excluded_names: Set of playlist names to exclude (currently preparing)
        
        Returns:
            List of selected playlists
        """
        if excluded_names is None:
            excluded_names = set()
        
        settings = self.config.get_settings()
        min_playlists = settings.get('min_playlists_per_rotation', DEFAULT_MIN_PLAYLISTS)
        max_playlists = settings.get('max_playlists_per_rotation', DEFAULT_MAX_PLAYLISTS)

        all_playlists = self.db.get_enabled_playlists()
        total_enabled = len(all_playlists)
        
        # Filter to only include playlists in config AND not currently preparing
        not_in_config = [p['name'] for p in all_playlists if p['name'] not in allowed_names]
        all_playlists = [p for p in all_playlists 
                        if p['name'] in allowed_names 
                        and p['name'] not in excluded_names]

        if len(all_playlists) == 0:
            logger.error(
                f"No eligible playlists available! "
                f"(enabled={total_enabled}, not_in_config={not_in_config}, "
                f"excluded_preparing={excluded_names})"
            )
            return []

        # Sort by last_played (oldest first) and priority
        # Playlists never played come first (NULLS FIRST is handled in SQL)

        # Select between min and max playlists
        num_to_select = min(len(all_playlists), max_playlists)
        num_to_select = max(num_to_select, min_playlists)
        # Cap at available playlists (can't select more than we have)
        num_to_select = min(num_to_select, len(all_playlists))
        if num_to_select < min_playlists:
            logger.error(f"Only {len(all_playlists)} playlists available, fewer than minimum {min_playlists} — blocking rotation")
            return []

        selected = all_playlists[:num_to_select]

        logger.info(f"Auto-selected {len(selected)} playlists: {[p['name'] for p in selected]} (excluded from preparation: {excluded_names})")
        return selected
    