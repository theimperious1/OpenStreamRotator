import logging
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
        
        Args:
            manual_selection: Optional list of playlist names to manually select
        
        Returns:
            List of playlist dictionaries to play
        """
        # Get allowed playlist names from config
        config_playlists = self.config.get_playlists()
        allowed_names = {p['name'] for p in config_playlists if p.get('enabled', True)}
        
        if manual_selection:
            return self._select_manual(manual_selection, allowed_names)
        else:
            return self._select_automatic(allowed_names)

    def _select_manual(self, manual_selection: List[str], allowed_names: set) -> List[Dict]:
        """
        Manually select specific playlists.
        
        Args:
            manual_selection: List of playlist names to select
            allowed_names: Set of playlist names allowed by config
        
        Returns:
            List of selected playlists
        """
        all_playlists = self.db.get_enabled_playlists()
        selected = [p for p in all_playlists if p['name'] in manual_selection and p['name'] in allowed_names]
        logger.info(f"Manual selection: {[p['name'] for p in selected]}")
        return selected

    def _select_automatic(self, allowed_names: set) -> List[Dict]:
        """
        Automatically select playlists based on rotation strategy.
        
        Args:
            allowed_names: Set of playlist names allowed by config
        
        Returns:
            List of selected playlists
        """
        settings = self.config.get_settings()
        min_playlists = settings.get('min_playlists_per_rotation', 2)
        max_playlists = settings.get('max_playlists_per_rotation', 4)

        all_playlists = self.db.get_enabled_playlists()
        
        # Filter to only include playlists in config
        all_playlists = [p for p in all_playlists if p['name'] in allowed_names]

        if len(all_playlists) == 0:
            logger.error("No enabled playlists available!")
            return []

        # Sort by last_played (oldest first) and priority
        # Playlists never played come first (NULLS FIRST is handled in SQL)

        # Select between min and max playlists
        num_to_select = min(len(all_playlists), max_playlists)
        num_to_select = max(num_to_select, min_playlists)

        selected = all_playlists[:num_to_select]

        logger.info(f"Auto-selected {len(selected)} playlists: {[p['name'] for p in selected]}")
        return selected
