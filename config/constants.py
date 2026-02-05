"""
Application-wide constants for stream automation.
Centralized location for magic numbers, strings, and IDs.
"""

# Kick Platform
KICK_FALLBACK_CATEGORY_ID = 15  # "Just Chatting" category ID on Kick

# API Timeouts (seconds)
API_TIMEOUT_GENERAL = 10
API_TIMEOUT_DOWNLOAD = 3600  # 1 hour for yt-dlp downloads

# Check Intervals (seconds)
CHECK_INTERVAL_MAIN_LOOP = 1  # Main loop responsiveness
CHECK_INTERVAL_TWITCH_API = 60  # Twitch live status checks

# Playback Skip Detection
SKIP_DETECTION_MARGIN_MS = 5000  # 5 second margin for VLC/OBS reporting variations

# Video File Extensions
VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.avi', '.webm', '.flv', '.mov', '.webm')

# Default Paths (can be overridden in config)
DEFAULT_VIDEO_FOLDER = 'C:/stream_videos/'
DEFAULT_NEXT_ROTATION_FOLDER = 'C:/stream_videos_next/'

# OBS Scene Names (must match OBS configuration)
# These can be overridden via environment variables
DEFAULT_SCENE_LIVE = "Pause screen"
DEFAULT_SCENE_OFFLINE = "Stream"
DEFAULT_SCENE_CONTENT_SWITCH = "content-switch"
DEFAULT_VLC_SOURCE_NAME = "Playlist"

# Playlist Constraints
DEFAULT_MIN_PLAYLISTS = 2
DEFAULT_MAX_PLAYLISTS = 4
DEFAULT_ROTATION_HOURS = 12

# Discord Notification Colors (hex values without 0x prefix)
COLOR_SUCCESS = 0x00FF00
COLOR_ERROR = 0xFF0000
COLOR_WARNING = 0xFF9900
COLOR_INFO = 0x0099FF
COLOR_STREAM_LIVE = 0x9146FF
COLOR_ROTATION_START = 0xFFA500
COLOR_NEXT_READY = 0x00FF00
