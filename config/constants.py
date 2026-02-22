"""
Application-wide constants for stream automation.
Centralized location for magic numbers, strings, and IDs.
"""

import os
import sys

# Project root directory — when frozen by PyInstaller the exe unpacks to a temp
# dir so __file__ no longer points at the real project folder.  Use the
# directory containing the exe instead.
if getattr(sys, 'frozen', False):
    _PROJECT_ROOT = os.path.dirname(sys.executable)
else:
    _PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Kick Platform
KICK_FALLBACK_CATEGORY_ID = 15  # "Just Chatting" category ID on Kick

# Video File Extensions
VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.avi', '.webm', '.flv', '.mov')

# Default Paths (can be overridden in config)
DEFAULT_VIDEO_FOLDER = os.path.join(_PROJECT_ROOT, 'content', 'live', '')
DEFAULT_NEXT_ROTATION_FOLDER = os.path.join(_PROJECT_ROOT, 'content', 'pending', '')
DEFAULT_FALLBACK_FOLDER = os.path.join(_PROJECT_ROOT, 'content', 'fallback', '')
DEFAULT_PAUSE_IMAGE = os.path.join(_PROJECT_ROOT, 'content', 'pause', 'default.png')
DEFAULT_ROTATION_IMAGE = os.path.join(_PROJECT_ROOT, 'content', 'rotation', 'default.png')

# OBS Scene Names (must match OBS configuration)
# These can be overridden via environment variables
DEFAULT_SCENE_PAUSE = "OSR Pause screen"
DEFAULT_SCENE_STREAM = "OSR Stream"
DEFAULT_SCENE_ROTATION_SCREEN = "OSR Rotation screen"
DEFAULT_VLC_SOURCE_NAME = "OSR Playlist"
DEFAULT_ALERT_SOURCE_NAME = "OSR Alert"

# Fallback system
DEFAULT_FALLBACK_FAILURE_THRESHOLD = 3

# Playlist Constraints
DEFAULT_MIN_PLAYLISTS = 2
DEFAULT_MAX_PLAYLISTS = 4

# Discord Notification Colors (hex values without 0x prefix)
COLOR_SUCCESS = 0x00FF00
COLOR_ERROR = 0xFF0000
COLOR_WARNING = 0xFF9900
COLOR_INFO = 0x0099FF
COLOR_STREAM_LIVE = 0x9146FF
COLOR_ROTATION_START = 0xFFA500
COLOR_NEXT_READY = 0x00FF00
COLOR_MUTED = 0x808080
COLOR_FALLBACK = 0xFF6600
