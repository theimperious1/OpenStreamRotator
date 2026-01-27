import sqlite3
import json
import os
from datetime import datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_path: Optional[str] = None):
        # Use core directory if not provided
        if db_path is None:
            core_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(core_dir, "stream_data.db")
        
        self.db_path = db_path
        self.conn = None
        self.init_database()

    def connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return self.conn

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def init_database(self):
        """Initialize database tables."""
        conn = self.connect()
        cursor = conn.cursor()

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
                filename TEXT NOT NULL,
                youtube_id TEXT,
                title TEXT,
                duration_seconds INTEGER,
                last_played TIMESTAMP,
                play_count INTEGER DEFAULT 0,
                file_size_mb INTEGER,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (playlist_id) REFERENCES playlists(id),
                UNIQUE(playlist_id, filename)
            )
        """)

        # Rotation sessions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rotation_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ended_at TIMESTAMP,
                suspended_at TIMESTAMP,
                suspension_data TEXT,
                playlists_selected TEXT,
                total_videos INTEGER,
                total_size_mb INTEGER,
                total_duration_seconds INTEGER DEFAULT 0,
                estimated_finish_time TIMESTAMP,
                download_trigger_time TIMESTAMP,
                stream_title TEXT,
                playback_seconds INTEGER DEFAULT 0
            )
        """)

        # Playback log table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playback_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id INTEGER,
                session_id INTEGER,
                played_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (video_id) REFERENCES videos(id),
                FOREIGN KEY (session_id) REFERENCES rotation_sessions(id)
            )
        """)

        conn.commit()
        self.close()
        logger.info("Database initialized successfully")

    def add_playlist(self, name: str, youtube_url: str, enabled: bool = True, priority: int = 1) -> Optional[int]:
        """Add a new playlist to the database."""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO playlists (name, youtube_url, enabled, priority)
                VALUES (?, ?, ?, ?)
            """, (name, youtube_url, enabled, priority))
            conn.commit()
            playlist_id = cursor.lastrowid
            logger.info(f"Added playlist: {name}")
            return playlist_id
        except sqlite3.IntegrityError:
            logger.warning(f"Playlist already exists: {name}")
            cursor.execute("SELECT id FROM playlists WHERE name = ?", (name,))
            return cursor.fetchone()[0]
        finally:
            self.close()

    def get_enabled_playlists(self) -> List[Dict]:
        """Get all enabled playlists."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM playlists 
            WHERE enabled = 1
            ORDER BY last_played ASC NULLS FIRST, priority DESC
        """)

        playlists = [dict(row) for row in cursor.fetchall()]
        self.close()
        return playlists

    def get_playlist(self, playlist_id: int) -> Optional[Dict]:
        """Get a specific playlist by ID."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM playlists WHERE id = ?", (playlist_id,))
        row = cursor.fetchone()
        self.close()

        if row:
            return dict(row)
        return None
        
    def update_playlist_played(self, playlist_id: int):
        """Update playlist's last_played timestamp and increment play_count."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE playlists 
            SET last_played = ?, 
                play_count = play_count + 1,
                updated_at = ?
            WHERE id = ?
        """, (datetime.now(), datetime.now(), playlist_id))

        conn.commit()
        self.close()

    def add_video(self, playlist_id: int, filename: str, title: Optional[str] = None,
                  file_size_mb: Optional[int] = None, duration_seconds: Optional[int] = None) -> Optional[int]:
        """Add a video to the database."""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO videos (playlist_id, filename, title, file_size_mb, duration_seconds)
                VALUES (?, ?, ?, ?, ?)
            """, (playlist_id, filename, title, file_size_mb, duration_seconds))
            conn.commit()
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
        finally:
            self.close()

    def get_videos_by_playlist(self, playlist_id: int) -> List[Dict]:
        """Get all videos for a specific playlist."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM videos 
            WHERE playlist_id = ?
        """, (playlist_id,))

        videos = [dict(row) for row in cursor.fetchall()]
        self.close()
        return videos

    def create_rotation_session(self, playlists_selected: List[int],
                                stream_title: str,
                                total_duration_seconds: int = 0,
                                estimated_finish_time: Optional[datetime] = None,
                                download_trigger_time: Optional[datetime] = None) -> Optional[int]:
        """Create a new rotation session."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO rotation_sessions (playlists_selected, stream_title, total_duration_seconds, 
                                          estimated_finish_time, download_trigger_time)
            VALUES (?, ?, ?, ?, ?)
        """, (json.dumps(playlists_selected), stream_title, total_duration_seconds,
              estimated_finish_time, download_trigger_time))

        conn.commit()
        session_id = cursor.lastrowid
        self.close()
        return session_id

    def get_session_by_id(self, session_id: int) -> Optional[Dict]:
        """Get a specific session by ID."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM rotation_sessions 
            WHERE id = ?
        """, (session_id,))

        row = cursor.fetchone()
        self.close()

        if row:
            return dict(row)
        return None

    def get_current_session(self) -> Optional[Dict]:
        """Get the current active rotation session."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM rotation_sessions 
            WHERE ended_at IS NULL 
            ORDER BY started_at DESC 
            LIMIT 1
        """)

        row = cursor.fetchone()
        self.close()

        if row:
            return dict(row)
        return None

    def update_session_playback(self, session_id: int, playback_seconds: int):
        """Update the playback time for a session."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE rotation_sessions 
            SET playback_seconds = ?
            WHERE id = ?
        """, (playback_seconds, session_id))

        conn.commit()
        self.close()

    def update_session_times(self, session_id: int, estimated_finish_time: str, download_trigger_time: str):
        """Update estimated_finish_time and download_trigger_time for a session (for skip detection recalculation)."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE rotation_sessions 
            SET estimated_finish_time = ?, download_trigger_time = ?
            WHERE id = ?
        """, (estimated_finish_time, download_trigger_time, session_id))

        conn.commit()
        self.close()

    def end_session(self, session_id: int):
        """Mark a rotation session as ended."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE rotation_sessions 
            SET ended_at = ?
            WHERE id = ?
        """, (datetime.now(), session_id))

        conn.commit()
        self.close()

    def suspend_session(self, session_id: int, suspension_data: Dict) -> bool:
        """Suspend a rotation session (pause it for manual override)."""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE rotation_sessions 
                SET suspended_at = ?, suspension_data = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), json.dumps(suspension_data), session_id))

            conn.commit()
            logger.info(f"Suspended session {session_id}: {suspension_data}")
            return True
        except Exception as e:
            logger.error(f"Failed to suspend session: {e}")
            return False
        finally:
            self.close()

    def resume_session(self, session_id: int) -> bool:
        """Resume a suspended rotation session."""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE rotation_sessions 
                SET suspended_at = NULL, suspension_data = NULL
                WHERE id = ?
            """, (session_id,))

            conn.commit()
            logger.info(f"Resumed session {session_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to resume session: {e}")
            return False
        finally:
            self.close()

    def get_suspended_session(self) -> Optional[Dict]:
        """Get the most recent suspended rotation session."""
        conn = self.connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM rotation_sessions 
            WHERE suspended_at IS NOT NULL AND ended_at IS NULL
            ORDER BY suspended_at DESC 
            LIMIT 1
        """)

        row = cursor.fetchone()
        self.close()

        if row:
            return dict(row)
        return None

    def update_session_column(self, session_id: int, column_name: str, value: str) -> bool:
        """Update a specific column in a session."""
        conn = self.connect()
        cursor = conn.cursor()

        try:
            # Build dynamic query based on column name (safe for known columns only)
            allowed_columns = ['suspension_data', 'suspension_notes']
            if column_name not in allowed_columns:
                logger.error(f"Invalid column name: {column_name}")
                return False
            
            query = f"UPDATE rotation_sessions SET {column_name} = ? WHERE id = ?"
            cursor.execute(query, (value, session_id))
            conn.commit()
            logger.info(f"Updated session {session_id} column {column_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to update session column: {e}")
            return False
        finally:
            self.close()

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