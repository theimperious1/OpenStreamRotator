# OpenStreamRotator

Fully automated 24/7 stream rerun system. Downloads YouTube playlists, plays them through VLC in OBS, rotates content when finished, and manages stream metadata on Kick and Twitch — all unattended.

## How It Works

1. **Playlist selection** — Each rotation, the system picks a configurable number of enabled playlists (prioritizing least-recently-played) and downloads their videos via yt-dlp into a pending folder.
2. **Content switch** — Once downloads finish, OBS briefly shows a transition scene while the live folder is swapped with the new content. VLC reloads and playback begins.
3. **File lock monitoring** — The system detects video transitions by checking OS-level file locks. On Windows, VLC locks the file it's currently playing. When a lock is released, that video is deleted and the next one is identified. No position tracking needed.
4. **Rotation trigger** — When all videos have been played and deleted (folder is empty), the system triggers the next rotation automatically.
5. **Stream metadata** — The stream title and category are updated on all enabled platforms (Kick, Twitch) each rotation. Categories update per-video based on which playlist the video came from.
6. **Temp playback** — If the current content runs out before the next rotation's downloads finish, the system temporarily plays already-downloaded files from the pending folder to avoid dead air.
7. **Twitch live detection** — If a configured target streamer goes live on Twitch, OBS switches to a pause screen. When they go offline, playback resumes automatically.

## Prerequisites

- **Python 3.10+**
- **OBS Studio 28+** with the OBS WebSocket plugin (v5, built into OBS 28+)
- **VLC Media Player** (used as a media source inside OBS)
- **yt-dlp** (installed automatically via pip, but must be on PATH for cookie extraction)

## Installation

```bash
git clone https://github.com/theimperious1/OpenStreamRotator.git
cd OpenStreamRotator
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

## Configuration

### Environment Variables (`.env`)

| Variable | Required | Default | Description |
|---|---|---|---|
| `ENABLE_TWITCH` | No | `false` | Enable Twitch integration |
| `TWITCH_CLIENT_ID` | If Twitch enabled | — | Twitch application client ID |
| `TWITCH_CLIENT_SECRET` | If Twitch enabled | — | Twitch application client secret |
| `TWITCH_USER_LOGIN` | If Twitch enabled | — | Your 24/7 Twitch channel username |
| `TARGET_TWITCH_STREAMER` | No | `zackrawrr` | Streamer whose live status pauses the rerun |
| `ENABLE_KICK` | No | `false` | Enable Kick integration |
| `KICK_CLIENT_ID` | If Kick enabled | — | Kick application client ID |
| `KICK_CLIENT_SECRET` | If Kick enabled | — | Kick application client secret |
| `KICK_CHANNEL_ID` | If Kick enabled | — | Your 24/7 Kick channel ID |
| `KICK_REDIRECT_URI` | If Kick enabled | `http://localhost:8080/callback` | OAuth redirect URI for Kick |
| `OBS_HOST` | No | `localhost` | OBS WebSocket host |
| `OBS_PORT` | No | `4455` | OBS WebSocket port |
| `OBS_PASSWORD` | Yes | — | OBS WebSocket password |
| `SCENE_LIVE` | No | `Pause screen` | OBS scene shown when target streamer is live |
| `SCENE_OFFLINE` | No | `Stream` | OBS scene for normal 24/7 playback |
| `SCENE_CONTENT_SWITCH` | No | `content-switch` | OBS scene shown during rotation transitions |
| `VLC_SOURCE_NAME` | No | `Playlist` | Name of the VLC media source in OBS |
| `DISCORD_WEBHOOK_URL` | No | — | Discord webhook for notifications |
| `YT_DLP_USE_COOKIES` | No | `false` | Use browser cookies for age-restricted videos |
| `YT_DLP_BROWSER_FOR_COOKIES` | No | `firefox` | Browser to extract cookies from (`chrome`, `firefox`, `brave`, `edge`, etc.) |
| `DEBUG_MODE_ENABLED` | No | `false` | Enable verbose debug logging |

### Playlists (`config/playlists.json`)

```json
{
  "playlists": [
    {
      "name": "MY PLAYLIST",
      "url": "https://www.youtube.com/playlist?list=PLxxxxxx",
      "category": "Just Chatting",
      "enabled": true,
      "priority": 1
    }
  ],
  "settings": {
    "download_buffer_minutes": 30,
    "video_folder": "C:/path/to/videos/live",
    "next_rotation_folder": "C:/path/to/videos/pending",
    "check_config_interval": 60,
    "min_playlists_per_rotation": 2,
    "max_playlists_per_rotation": 2,
    "download_retry_attempts": 5,
    "stream_title_template": "24/7 Reruns | {GAMES} | ",
    "yt_dlp_verbose": false
  }
}
```

**Playlist fields:**

| Field | Description |
|---|---|
| `name` | Display name used in stream titles and logs |
| `url` | YouTube playlist URL |
| `category` | Platform category set when videos from this playlist are playing (e.g. `"Just Chatting"`, `"Final Fantasy XIV"`). Falls back to the playlist name if not set. |
| `enabled` | Whether this playlist is available for rotation selection |
| `priority` | Selection priority (lower = higher priority among equally-old playlists) |

**Settings fields:**

| Field | Description |
|---|---|
| `download_buffer_minutes` | Minutes before estimated content end to start downloading the next rotation |
| `video_folder` | Absolute path to the live playback folder (VLC reads from here) |
| `next_rotation_folder` | Absolute path to the pending/download folder |
| `check_config_interval` | Seconds between config file re-reads |
| `min_playlists_per_rotation` | Minimum playlists selected per rotation |
| `max_playlists_per_rotation` | Maximum playlists selected per rotation |
| `download_retry_attempts` | Number of retry attempts for failed downloads |
| `stream_title_template` | Title template. `{GAMES}` is replaced with playlist names joined by ` \| ` |
| `yt_dlp_verbose` | Enable verbose yt-dlp output in logs |

## OBS Setup

You need **three scenes** and one **VLC media source**:

### Scenes

1. **`Stream`** (default name, configurable via `SCENE_OFFLINE`) — The main playback scene. Should contain your VLC media source and any overlays. This is what viewers see during normal 24/7 operation.
2. **`Pause screen`** (default name, configurable via `SCENE_LIVE`) — Shown when the target streamer goes live. Typically a static image telling viewers the main stream is live.
3. **`content-switch`** (default name, configurable via `SCENE_CONTENT_SWITCH`) — Brief transition scene shown while content folders are being swapped. Can be a loading animation or static image.

### VLC Media Source

Add a **VLC Video Source** (not a regular Media Source) named **`Playlist`** (configurable via `VLC_SOURCE_NAME`) to your `Stream` scene with these settings:

- **Playlist directory**: Point it to your `video_folder` path from `playlists.json`
- **Loop**: Enabled (the system handles advancement by deleting played files; loop ensures VLC keeps playing)
- **Shuffle**: Disabled (the system controls ordering via filename prefixes)

> The VLC source plugin for OBS must be installed separately if not already present.

## Kick Setup

When Kick integration is enabled, the bot uses OAuth for authentication:

1. On first startup, a browser window opens automatically to the Kick authorization page.
2. Log in and authorize the application.
3. You'll be redirected to a page with an authorization code.
4. Paste the code into the terminal when prompted.
5. Tokens are saved and refreshed automatically for subsequent runs.

## Running

```bash
python main.py
```

The system will:
1. Connect to OBS via WebSocket
2. Verify all required scenes and sources exist
3. Sync playlist configuration to the database
4. Resume any interrupted session or start a new rotation
5. Begin the main automation loop

### What Happens Each Rotation

1. Playlists are selected (least-recently-played first, excluding currently playing and currently downloading playlists)
2. Videos are downloaded via yt-dlp into the pending folder
3. When downloads complete, OBS switches to the content-switch scene
4. The live folder is cleared and pending content is moved in
5. Videos are renamed with ordering prefixes (`01_video.mp4`, `02_video.mp4`, etc.) so VLC plays them grouped by playlist
6. VLC reloads, OBS switches back to the playback scene
7. Stream title and category are updated on all enabled platforms
8. The file lock monitor begins tracking playback; as each video finishes, it's deleted
9. When all content is consumed, the cycle repeats

### Temp Playback

If the current rotation's content runs out before the next rotation's downloads finish, the system enters **temp playback mode**:

- Already-downloaded files in the pending folder are played immediately
- Videos are deleted after playing (an archive file prevents yt-dlp from re-downloading them)
- When all downloads complete, a normal content switch happens

This prevents dead air during long downloads.

### Crash Recovery

The system tracks session state in a local SQLite database. If the process is restarted:

- If videos remain in the live folder, playback resumes from where VLC picks up
- If a temp playback session was active, it's restored
- If mid-download, downloads resume (yt-dlp handles partial file resumption)

### Skipping Videos

You can skip videos directly in OBS by advancing the VLC source. The file lock monitor will detect the transition naturally — the skipped video's lock is released, it gets deleted, and the next video is tracked.

### Graceful Shutdown

Press `Ctrl+C` to stop. The system handles the interrupt cleanly and shuts down.

## Discord Notifications

If `DISCORD_WEBHOOK_URL` is set, the system sends notifications for:

- Rotation starts (with playlist names and video count)
- Content switches
- Download progress and completion
- Stream title/category update failures
- Target streamer going live/offline
- Errors and warnings

## Twitch Live Detection

When `ENABLE_TWITCH` is set to `true`, the system polls the Twitch API every 60 seconds to check if `TARGET_TWITCH_STREAMER` is live:

- **Streamer goes live** → OBS switches to the pause scene, rotations are postponed
- **Streamer goes offline** → OBS switches back to the playback scene, normal operation resumes

This is designed for 24/7 rerun channels that should yield to the main streamer.

## Reset State

Use the provided reset script to wipe all state and start fresh:

**Windows:**
```bash
reset_state.bat
```

**Linux/Mac:**
```bash
chmod +x reset_state.sh
./reset_state.sh
```

This deletes the SQLite database and all downloaded videos from both the live and pending folders (paths are read from `playlists.json`).

## Project Structure

```
OpenStreamRotator/
├── main.py                          # Entry point
├── config/
│   ├── config_manager.py            # Loads .env and playlists.json
│   ├── constants.py                 # Application constants
│   └── playlists.json               # Playlist and settings configuration
├── controllers/
│   ├── automation_controller.py     # Main orchestration loop
│   └── obs_controller.py            # OBS WebSocket interface
├── core/
│   └── database.py                  # SQLite session and playlist tracking
├── handlers/
│   ├── content_switch_handler.py    # OBS scene transitions during rotation
│   ├── rotation_handler.py          # Background download triggers and rotation prep
│   └── temp_playback_handler.py     # Temp playback during long downloads
├── integrations/
│   └── platforms/
│       ├── base/stream_platform.py  # Platform interface
│       ├── kick.py                  # Kick API integration
│       └── twitch.py                # Twitch API integration
├── managers/
│   ├── platform_manager.py          # Multi-platform broadcast orchestration
│   ├── playlist_manager.py          # Folder management and video renaming
│   └── stream_manager.py            # Stream title and category updates
├── playback/
│   └── file_lock_monitor.py         # File-lock-based video transition detection
├── services/
│   ├── notification_service.py      # Discord webhook notifications
│   └── twitch_live_checker.py       # Twitch live status polling
├── utils/
│   ├── playlist_selector.py         # Rotation playlist selection logic
│   ├── video_downloader.py          # yt-dlp download manager
│   └── video_processor.py           # Video file processing utilities
└── videos/
    ├── live/                        # Current playback folder (VLC reads from here)
    ├── pending/                     # Next rotation downloads
    │   ├── archive.txt              # yt-dlp download archive (prevents re-downloads)
    │   └── temp/                    # yt-dlp partial download fragments
    └── content-switch/              # Assets for the content-switch OBS scene
```

## License

This project is open source. See the repository for license details.
