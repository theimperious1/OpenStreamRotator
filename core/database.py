import sqlite3
import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Any, List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    
    @staticmethod
    def parse_json_field(value, default=None) -> Any:
        """Parse a JSON string field from the database, returning default if empty/None.
        
        Handles the common pattern where a column stores JSON but may be
        returned as a string (from sqlite3.Row) or already parsed (from dict).
        
        Args:
            value: The raw value from the database (str, list, dict, or None)
            default: Default value if parsing fails or value is None
            
        Returns:
            Parsed Python object, or default
        """
        if default is None:
            default = []
        if value is None:
            return default
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return default
        return value

    def __init__(self, db_path: Optional[str] = None):
        # Use core directory if not provided
        if db_path is None:
            core_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(core_dir, "stream_data.db")
        
        self.db_path = db_path
        self._lock = threading.RLock()
        # Persistent connection — check_same_thread=False since we protect with _lock
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.init_database()

    @contextmanager
    def _cursor(self):
        """Thread-safe cursor context manager.
        
        Acquires the lock, yields a cursor, and commits on success.
        Uses RLock so nested calls (e.g., log_playback -> get_video_by_filename) are safe.
        """
        with self._lock:
            cursor = self.conn.cursor()
            try:
                yield cursor
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def close(self):
        """Close the persistent database connection (call only on shutdown)."""
        with self._lock:
            if self.conn:
                self.conn.close()
                self.conn = None

    def init_database(self):
        """Initialize database tables."""
        with self._cursor() as cursor:
            # Playlists table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS playlists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    youtube_url TEXT NOT NULL,
                    last_played TIMESTAMP,
                    play_count INTEGER DEFAULT 0,
                    enabled BOOLEAN DEFAULT 1,
                    priority INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Videos table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    playlist_id INTEGER NOT NULL,
                    playlist_name TEXT,
                    filename TEXT NOT NULL,
                    title TEXT,
                    duration_seconds INTEGER,
                    file_size_mb INTEGER,
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (playlist_id) REFERENCES playlists(id),
                    UNIQUE(playlist_id, filename)
                )
            """)

            # Add playlist_name column to existing videos table if it doesn't exist
            cursor.execute("""
                PRAGMA table_info(videos)
            """)
            columns = [col[1] for col in cursor.fetchall()]
            if 'playlist_name' not in columns:
                try:
                    cursor.execute("""
                        ALTER TABLE videos ADD COLUMN playlist_name TEXT
                    """)
                    logger.info("Added playlist_name column to videos table")
                except sqlite3.OperationalError:
                    logger.debug("playlist_name column already exists")

            # Rotation sessions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rotation_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ended_at TIMESTAMP,
                    playlists_selected TEXT,
                    total_duration_seconds INTEGER DEFAULT 0,
                    stream_title TEXT,
                    is_current BOOLEAN DEFAULT 0,
                    current_playlists TEXT,
                    next_playlists TEXT,
                    next_playlists_status TEXT
                )
            """)

            # Add new columns to existing table if they don't exist
            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN current_playlists TEXT")
                logger.info("Added current_playlists column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("current_playlists column already exists")

            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN next_playlists TEXT")
                logger.info("Added next_playlists column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("next_playlists column already exists")

            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN next_playlists_status TEXT")
                logger.info("Added next_playlists_status column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("next_playlists_status column already exists")

            # Temp playback state columns for crash recovery
            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN temp_playback_active BOOLEAN DEFAULT 0")
                logger.info("Added temp_playback_active column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("temp_playback_active column already exists")

            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN temp_playback_playlist TEXT")
                logger.info("Added temp_playback_playlist column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("temp_playback_playlist column already exists")

            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN temp_playback_position INTEGER DEFAULT 0")
                logger.info("Added temp_playback_position column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("temp_playback_position column already exists")

            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN temp_playback_folder TEXT")
                logger.info("Added temp_playback_folder column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("temp_playback_folder column already exists")

            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN temp_playback_cursor_ms INTEGER DEFAULT 0")
                logger.info("Added temp_playback_cursor_ms column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("temp_playback_cursor_ms column already exists")

            # Playback position tracking for crash recovery
            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN playback_cursor_ms INTEGER DEFAULT 0")
                logger.info("Added playback_cursor_ms column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("playback_cursor_ms column already exists")

            try:
                cursor.execute("ALTER TABLE rotation_sessions ADD COLUMN playback_current_video TEXT")
                logger.info("Added playback_current_video column to rotation_sessions table")
            except sqlite3.OperationalError:
                logger.debug("playback_current_video column already exists")

            # Playback log table - records each video transition for historical audit
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS playback_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id INTEGER,
                    session_id INTEGER,
                    video_filename TEXT,
                    playlist_name TEXT,
                    played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (video_id) REFERENCES videos(id),
                    FOREIGN KEY (session_id) REFERENCES rotation_sessions(id)
                )
            """)

            logger.info("Database initialized successfully")

    def add_playlist(self, name: str, youtube_url: str, enabled: bool = True, priority: int = 1) -> Optional[int]:
        """Add a new playlist to the database."""
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    INSERT INTO playlists (name, youtube_url, enabled, priority)
                    VALUES (?, ?, ?, ?)
                """, (name, youtube_url, enabled, priority))
                playlist_id = cursor.lastrowid
                logger.info(f"Added playlist: {name}")
                return playlist_id
            except sqlite3.IntegrityError:
                logger.warning(f"Playlist already exists: {name}")
                cursor.execute("SELECT id FROM playlists WHERE name = ?", (name,))
                return cursor.fetchone()[0]

    def get_enabled_playlists(self) -> List[Dict]:
        """Get all enabled playlists."""
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM playlists 
                WHERE enabled = 1
                ORDER BY last_played ASC NULLS FIRST, priority DESC
            """)

            playlists = [dict(row) for row in cursor.fetchall()]
            return playlists

    def get_playlist(self, playlist_id: int) -> Optional[Dict]:
        """Get a specific playlist by ID."""
        with self._cursor() as cursor:
            cursor.execute("SELECT * FROM playlists WHERE id = ?", (playlist_id,))
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    def update_playlist_played(self, playlist_id: int):
        """Update playlist's last_played timestamp and increment play_count."""
        with self._cursor() as cursor:
            cursor.execute("""
                UPDATE playlists 
                SET last_played = ?, 
                    play_count = play_count + 1,
                    updated_at = ?
                WHERE id = ?
            """, (datetime.now(), datetime.now(), playlist_id))


    def log_playback(self, video_filename: str, session_id: Optional[int] = None) -> None:
        """Record a video playback event in the playback log.
        
        Args:
            video_filename: Original filename of the video (without prefix)
            session_id: Current rotation session ID
        """
        with self._cursor() as cursor:
            try:
                # Look up video_id and playlist_name from videos table
                video_id = None
                playlist_name = None
                video = self.get_video_by_filename(video_filename)
                if video:
                    video_id = video.get('id')
                    playlist_name = video.get('playlist_name')

                cursor.execute("""
                    INSERT INTO playback_log (video_id, session_id, video_filename, playlist_name)
                    VALUES (?, ?, ?, ?)
                """, (video_id, session_id, video_filename, playlist_name))
                logger.debug(f"Logged playback: {video_filename} (playlist={playlist_name})")
            except Exception as e:
                logger.warning(f"Failed to log playback for {video_filename}: {e}")

    def add_video(self, playlist_id: int, filename: str, title: Optional[str] = None,
                  file_size_mb: Optional[int] = None, duration_seconds: Optional[int] = None,
                  playlist_name: Optional[str] = None) -> Optional[int]:
        """Add a video to the database.
        
        Args:
            playlist_id: Database ID of the playlist
            filename: Video filename
            title: Video title
            file_size_mb: File size in MB
            duration_seconds: Video duration in seconds
            playlist_name: Name of the playlist from config (for category lookups)
        
        Returns:
            Video ID
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    INSERT INTO videos (playlist_id, playlist_name, filename, title, file_size_mb, duration_seconds)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (playlist_id, playlist_name, filename, title, file_size_mb, duration_seconds))
                video_id = cursor.lastrowid
                return video_id
            except sqlite3.IntegrityError:
                # Sanitize filename for logging (remove Unicode characters that cause encoding errors)
                safe_filename = filename.encode('ascii', 'ignore').decode('ascii')
                logger.warning(f"Video already exists: {safe_filename}")
                cursor.execute("""
                    SELECT id FROM videos 
                    WHERE playlist_id = ? AND filename = ?
                """, (playlist_id, filename))
                return cursor.fetchone()[0]

    def get_videos_by_playlist(self, playlist_id: int) -> List[Dict]:
        """Get all videos for a specific playlist."""
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM videos 
                WHERE playlist_id = ?
            """, (playlist_id,))

            videos = [dict(row) for row in cursor.fetchall()]
            return videos

    def get_video_by_filename(self, filename: str) -> Optional[Dict]:
        """Get a video by its filename (searches across all playlists).
        
        Args:
            filename: Video filename
        
        Returns:
            Video dict with playlist_name, or None if not found
        """
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM videos 
                WHERE filename = ?
                LIMIT 1
            """, (filename,))

            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    def create_rotation_session(self, playlists_selected: List[int],
                                stream_title: str,
                                total_duration_seconds: int = 0) -> Optional[int]:
        """Create a new rotation session with clean state.
        
        This ensures only one session is marked as current at a time.
        Any previously current session is marked as inactive.
        The new session starts with clean next_playlists to prevent
        stale playlist exclusions in the selector.
        """
        with self._cursor() as cursor:
            # Mark any existing current session as inactive (preserving suspension state)
            cursor.execute("""
                UPDATE rotation_sessions 
                SET is_current = 0 
                WHERE is_current = 1
            """)

            # Create new session with clean state (next_playlists starts null)
            cursor.execute("""
                INSERT INTO rotation_sessions (playlists_selected, stream_title, total_duration_seconds, 
                                              is_current, current_playlists, next_playlists)
                VALUES (?, ?, ?, 1, NULL, NULL)
            """, (json.dumps(playlists_selected), stream_title, total_duration_seconds))

            session_id = cursor.lastrowid
            logger.info(f"Created new rotation session {session_id} (marked previous sessions as inactive)")
            return session_id

    def update_session_stream_title(self, session_id: int, stream_title: str) -> None:
        """Update the stream title for a rotation session."""
        with self._cursor() as cursor:
            cursor.execute("""
                UPDATE rotation_sessions SET stream_title = ? WHERE id = ?
            """, (stream_title, session_id))

    def save_playback_position(self, session_id: int, cursor_ms: int, current_video: Optional[str] = None) -> None:
        """Save the current playback position for crash recovery.
        
        Called every second from the main loop to keep position up to date.
        
        Args:
            session_id: Current rotation session ID
            cursor_ms: Current playback position in milliseconds
            current_video: Filename of the currently playing video
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    UPDATE rotation_sessions 
                    SET playback_cursor_ms = ?, playback_current_video = ?
                    WHERE id = ?
                """, (cursor_ms, current_video, session_id))
            except Exception as e:
                logger.debug(f"Failed to save playback position: {e}")

    def clear_playback_position(self, session_id: int) -> None:
        """Clear saved playback position (e.g. on rotation switch)."""
        self.save_playback_position(session_id, 0, None)

    def get_session_by_id(self, session_id: int) -> Optional[Dict]:
        """Get a specific session by ID."""
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM rotation_sessions 
                WHERE id = ?
            """, (session_id,))

            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    def get_current_session(self) -> Optional[Dict]:
        """Get the current active rotation session."""
        with self._cursor() as cursor:
            cursor.execute("""
                SELECT * FROM rotation_sessions 
                WHERE is_current = 1 
                LIMIT 1
            """)

            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    def end_session(self, session_id: int):
        """Mark a rotation session as ended and update playlists' last_played timestamp."""
        with self._cursor() as cursor:
            # Get playlists used in this session
            cursor.execute("""
                SELECT playlists_selected FROM rotation_sessions WHERE id = ?
            """, (session_id,))
            result = cursor.fetchone()

            if result:
                playlists_json = result[0]
                if playlists_json:
                    try:
                        playlist_ids = json.loads(playlists_json)
                        # Update last_played for all playlists in this session
                        now = datetime.now()
                        for playlist_id in playlist_ids:
                            cursor.execute("""
                                UPDATE playlists 
                                SET last_played = ?, 
                                    play_count = play_count + 1,
                                    updated_at = ?
                                WHERE id = ?
                            """, (now, now, playlist_id))
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"Failed to parse playlists for session {session_id}: {e}")

            cursor.execute("""
                UPDATE rotation_sessions 
                SET ended_at = ?, is_current = 0
                WHERE id = ?
            """, (datetime.now(), session_id))




    def update_session_column(self, session_id: int, column_name: str, value: str) -> bool:
        """Update a specific column in a session."""
        with self._cursor() as cursor:
            try:
                # Build dynamic query based on column name (safe for known columns only)
                allowed_columns = ['suspension_data', 'suspension_notes']
                if column_name not in allowed_columns:
                    logger.error(f"Invalid column name: {column_name}")
                    return False

                query = f"UPDATE rotation_sessions SET {column_name} = ? WHERE id = ?"
                cursor.execute(query, (value, session_id))
                logger.info(f"Updated session {session_id} column {column_name}")
                return True
            except Exception as e:
                logger.error(f"Failed to update session column: {e}")
                return False

    def update_playlist_status(self, session_id: int, playlist_name: str, status: str = "PENDING") -> bool:
        """Update the status of a specific playlist in next_playlists_status.
        
        Args:
            session_id: Session ID
            playlist_name: Name of the playlist
            status: Status value (e.g., "PENDING", "COMPLETED")
        
        Returns:
            True if updated successfully
        """
        with self._cursor() as cursor:
            try:
                # Get current status
                cursor.execute("SELECT next_playlists_status FROM rotation_sessions WHERE id = ?", (session_id,))
                row = cursor.fetchone()

                if row and row[0]:
                    status_dict = json.loads(row[0])
                else:
                    status_dict = {}

                # Update the status for this playlist
                status_dict[playlist_name] = status

                # Save back to database
                cursor.execute(
                    "UPDATE rotation_sessions SET next_playlists_status = ? WHERE id = ?",
                    (json.dumps(status_dict), session_id)
                )
                logger.debug(f"Updated playlist '{playlist_name}' to {status} in session {session_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to update playlist status: {e}")
                return False

    def set_next_playlists(self, session_id: int, playlists: List[str]) -> bool:
        """Set the list of next playlists and initialize their status to PENDING.
        
        Args:
            session_id: Session ID
            playlists: List of playlist names
        
        Returns:
            True if updated successfully
        """
        with self._cursor() as cursor:
            try:
                # Store playlist names
                cursor.execute(
                    "UPDATE rotation_sessions SET next_playlists = ? WHERE id = ?",
                    (json.dumps(playlists), session_id)
                )

                # Initialize all playlists as PENDING
                status_dict = {pl: "PENDING" for pl in playlists}
                cursor.execute(
                    "UPDATE rotation_sessions SET next_playlists_status = ? WHERE id = ?",
                    (json.dumps(status_dict), session_id)
                )

                logger.debug(f"Set next_playlists to {playlists} in session {session_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to set next playlists: {e}")
                return False

    def set_current_playlists(self, session_id: int, playlists: List[str]) -> bool:
        """Set the list of current playlists for this session.
        
        Args:
            session_id: Session ID
            playlists: List of playlist names currently playing
        
        Returns:
            True if updated successfully
        """
        with self._cursor() as cursor:
            try:
                cursor.execute(
                    "UPDATE rotation_sessions SET current_playlists = ? WHERE id = ?",
                    (json.dumps(playlists), session_id)
                )
                logger.debug(f"Set current_playlists to {playlists} in session {session_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to set current playlists: {e}")
                return False

    def get_playlist_status(self, session_id: int, playlist_name: str) -> Optional[str]:
        """Get the status of a specific playlist.
        
        Args:
            session_id: Session ID
            playlist_name: Name of the playlist
        
        Returns:
            Status string ("PENDING", "COMPLETED", etc.) or None
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("SELECT next_playlists_status FROM rotation_sessions WHERE id = ?", (session_id,))
                row = cursor.fetchone()

                if row and row[0]:
                    status_dict = json.loads(row[0])
                    return status_dict.get(playlist_name)
                return None
            except Exception as e:
                logger.error(f"Failed to get playlist status: {e}")
                return None

    def get_next_playlists_status(self, session_id: int) -> Dict[str, str]:
        """Get all next playlist statuses for a session.
        
        Args:
            session_id: Session ID
        
        Returns:
            Dictionary mapping playlist names to their status
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("SELECT next_playlists_status FROM rotation_sessions WHERE id = ?", (session_id,))
                row = cursor.fetchone()

                if row and row[0]:
                    return json.loads(row[0])
                return {}
            except Exception as e:
                logger.error(f"Failed to get next playlists status: {e}")
                return {}

    def sync_playlists_from_config(self, config_playlists: List[Dict]):
        """Sync playlists from config file to database."""
        for playlist in config_playlists:
            if playlist.get('enabled', True):
                self.add_playlist(
                    name=playlist['name'],
                    youtube_url=playlist['url'],
                    enabled=True,
                    priority=playlist.get('priority', 1)
                )
        logger.info(f"Synced {len(config_playlists)} playlists from config")

    def initialize_next_playlists(self, session_id: int, playlist_names: List[str]):
        """Initialize next_playlists tracking for a session.
        
        Sets all playlists to PENDING status at download start.
        Thread-safe: runs in main thread after background thread queues the request.
        """
        if not session_id or not playlist_names:
            return False
        
        try:
            self.set_next_playlists(session_id, playlist_names)
            logger.info(f"Initialized next_playlists tracking in session {session_id}: {playlist_names}")
            return True
        except Exception as e:
            logger.warning(f"Failed to initialize next_playlists tracking: {e}")
            return False

    def complete_next_playlists(self, session_id: int, playlist_names: List[str]):
        """Mark all next_playlists as COMPLETED after successful download.
        
        Called in main thread after background download thread finishes.
        Thread-safe: runs in main thread.
        """
        if not session_id or not playlist_names:
            return False
        
        try:
            for playlist_name in playlist_names:
                self.update_playlist_status(session_id, playlist_name, "COMPLETED")
            logger.info(f"Updated database: marked {playlist_names} as COMPLETED in session {session_id}")
            return True
        except Exception as e:
            logger.warning(f"Failed to update playlist status in database: {e}")
            return False

    def get_playlists_with_ids_by_names(self, playlist_names: List[str]) -> List[Dict]:
        """Get full playlist data with database IDs by their names.
        
        Used when restoring prepared playlists - ensures we have both config data and DB IDs.
        Returns playlists with ''id'', ''name'', ''youtube_url'', and other database fields.
        """
        if not playlist_names:
            return []
        
        with self._cursor() as cursor:
            try:
                playlists = []
                for name in playlist_names:
                    cursor.execute("SELECT * FROM playlists WHERE name = ?", (name,))
                    row = cursor.fetchone()
                    if row:
                        playlists.append(dict(row))
                return playlists
            except Exception as e:
                logger.error(f"Failed to get playlists with IDs by names: {e}")
                return []

    def save_temp_playback_state(self, session_id: int, playlist: List[str], position: int, folder: str, cursor_ms: int = 0) -> bool:
        """Save temp playback state for crash recovery.
        
        Args:
            session_id: Session ID
            playlist: List of video filenames in the VLC playlist
            position: Current position in the playlist (which video)
            folder: Path to the temp playback folder (pending folder)
            cursor_ms: Current playback position within the video in milliseconds
        
        Returns:
            True if saved successfully
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    UPDATE rotation_sessions 
                    SET temp_playback_active = 1,
                        temp_playback_playlist = ?,
                        temp_playback_position = ?,
                        temp_playback_folder = ?,
                        temp_playback_cursor_ms = ?
                    WHERE id = ?
                """, (json.dumps(playlist), position, folder, cursor_ms, session_id))
                logger.info(f"Saved temp playback state: {len(playlist)} videos, position={position}, cursor={cursor_ms}ms")
                return True
            except Exception as e:
                logger.error(f"Failed to save temp playback state: {e}")
                return False

    def update_temp_playback_position(self, session_id: int, position: int) -> bool:
        """Update only the temp playback position (called on video transitions).
        
        Args:
            session_id: Session ID
            position: New position in the playlist
        
        Returns:
            True if updated successfully
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    UPDATE rotation_sessions 
                    SET temp_playback_position = ?,
                        temp_playback_cursor_ms = 0
                    WHERE id = ?
                """, (position, session_id))
                return True
            except Exception as e:
                logger.error(f"Failed to update temp playback position: {e}")
                return False

    def update_temp_playback_cursor(self, session_id: int, cursor_ms: int) -> bool:
        """Update the playback cursor position within current video (called periodically).
        
        Args:
            session_id: Session ID
            cursor_ms: Current playback position in milliseconds
        
        Returns:
            True if updated successfully
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    UPDATE rotation_sessions 
                    SET temp_playback_cursor_ms = ?
                    WHERE id = ?
                """, (cursor_ms, session_id))
                return True
            except Exception as e:
                logger.error(f"Failed to update temp playback cursor: {e}")
                return False

    def clear_temp_playback_state(self, session_id: int) -> bool:
        """Clear temp playback state when exiting temp playback normally.
        
        Args:
            session_id: Session ID
        
        Returns:
            True if cleared successfully
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    UPDATE rotation_sessions 
                    SET temp_playback_active = 0,
                        temp_playback_playlist = NULL,
                        temp_playback_position = NULL,
                        temp_playback_folder = NULL,
                        temp_playback_cursor_ms = NULL
                    WHERE id = ?
                """, (session_id,))
                logger.info(f"Cleared temp playback state for session {session_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to clear temp playback state: {e}")
                return False

    def get_temp_playback_state(self, session_id: int) -> Optional[Dict]:
        """Get temp playback state for recovery.
        
        Args:
            session_id: Session ID
        
        Returns:
            Dict with active, playlist, position, folder, cursor_ms
            or None if no temp playback was active
        """
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    SELECT temp_playback_active, temp_playback_playlist, temp_playback_position, temp_playback_folder, temp_playback_cursor_ms
                    FROM rotation_sessions 
                    WHERE id = ?
                """, (session_id,))
                row = cursor.fetchone()

                if row and row[0]:  # temp_playback_active is True
                    playlist = json.loads(row[1]) if row[1] else []
                    return {
                        'active': True,
                        'playlist': playlist,
                        'position': row[2] or 0,
                        'folder': row[3],
                        'cursor_ms': row[4] or 0
                    }
                return None
            except Exception as e:
                logger.error(f"Failed to get temp playback state: {e}")
                return None

    def validate_prepared_playlists_exist(self, session_id: int, pending_folder: str) -> bool:
        """
        Verify that all files for prepared playlists (next_playlists) actually exist in pending folder.
        
        Args:
            session_id: Session ID to check
            pending_folder: Path to pending/next rotation folder
        
        Returns:
            True if all prepared playlist files exist, False otherwise
        """
        
        with self._cursor() as cursor:
            try:
                cursor.execute("""
                    SELECT next_playlists FROM rotation_sessions 
                    WHERE id = ?
                """, (session_id,))

                row = cursor.fetchone()
                if not row or not row[0]:
                    # No prepared playlists, nothing to validate
                    return True

                next_playlists_json = row[0]
                next_playlists = self.parse_json_field(next_playlists_json, [])
                if not next_playlists:
                    logger.warning("Could not parse next_playlists JSON")
                    return False

                # Get all video files for these playlists
                expected_files = set()

                for playlist_name in next_playlists:
                    cursor.execute("""
                        SELECT filename FROM videos 
                        WHERE playlist_name = ?
                    """, (playlist_name,))

                    for row in cursor.fetchall():
                        expected_files.add(row[0])

                # Check if all files exist in pending folder
                if not os.path.exists(pending_folder):
                    logger.warning(f"Pending folder does not exist: {pending_folder}")
                    return False

                actual_files = set(os.listdir(pending_folder))

                missing_files = expected_files - actual_files
                if missing_files:
                    logger.warning(f"Prepared playlist files missing from pending folder: {list(missing_files)[:5]}")
                    return False

                logger.info(f"Validated {len(expected_files)} prepared playlist files exist in pending folder")
                return True

            except Exception as e:
                logger.error(f"Error validating prepared playlists: {e}")
                return False
