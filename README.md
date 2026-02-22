<p align="center">
  <img src=".github/assets/osr-banner-1500x500.png" alt="OpenStreamRotator" width="100%" />
</p>

Fully automated 24/7 stream rerun system. Downloads YouTube playlists, plays them through VLC in OBS, rotates content when finished, and manages stream metadata on Kick and Twitch — all unattended.

> **Like this project?** [Buy me a coffee ☕](https://buymeacoffee.com/theimperious1)

## How It Works

1. **Playlist selection** — Each rotation, the system picks a configurable number of enabled playlists (prioritizing least-recently-played) and downloads their videos via yt-dlp into a pending folder.
2. **Content switch** — Once downloads finish, OBS briefly shows a transition scene while the live folder is swapped with the new content. VLC reloads and playback begins.
3. **File lock monitoring** — The system detects video transitions by checking OS-level file locks. On Windows, VLC locks the file it's currently playing. When a lock is released, that video is deleted and the next one is identified. No position tracking needed.
4. **Rotation trigger** — When all videos have been played and deleted (folder is empty), the system triggers the next rotation automatically.
5. **Stream metadata** — The stream title and category are updated on all enabled platforms (Kick, Twitch) each rotation. Categories update per-video based on which playlist the video came from.
6. **Temp playback** — If the current content runs out before the next rotation's downloads finish, the system temporarily plays already-downloaded files from the pending folder to avoid dead air.
7. **Twitch live detection** — If a configured target streamer goes live on Twitch, OBS switches to a pause screen. When they go offline, playback resumes automatically.
8. **OBS freeze detection** — The system monitors OBS render output via WebSocket. If OBS stops rendering frames for ~60 seconds (process alive but frozen), it automatically kills OBS, clears crash sentinels, relaunches it, reconnects, and resumes streaming — all unattended.

## Prerequisites

- **Python 3.10+**
- **OBS Studio 28+** with the OBS WebSocket plugin (v5, built into OBS 28+)
- **VLC Media Player** (used as a media source inside OBS)
- **yt-dlp** (installed automatically via pip, but must be on PATH for cookie extraction)

## Installation

```bash
git clone https://github.com/theimperious1/OpenStreamRotator.git
cd OpenStreamRotator
```

### Quick Setup (recommended)

Run the interactive setup script — it walks you through every option and creates your `.env` file:

**Windows:**
```bash
setup.bat
```

**Linux/Mac:**
```bash
chmod +x setup.sh
./setup.sh
```

Then start OSR:

**Windows:**
```bash
run.bat
```

**Linux/Mac:**
```bash
./run.sh
```

### Manual Setup

If you prefer to configure everything yourself:

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
python main.py
```

## Configuration

### Environment Variables (`.env`)

| Variable | Required | Default | Description |
|---|---|---|---|
| `ENABLE_TWITCH` | No | `false` | Enable Twitch integration (optional — not required if only using Kick) |
| `TWITCH_CLIENT_ID` | If Twitch enabled | — | Twitch application client ID |
| `TWITCH_CLIENT_SECRET` | If Twitch enabled | — | Twitch application client secret |
| `TWITCH_USER_LOGIN` | If Twitch enabled | — | Your 24/7 Twitch channel username |
| `TWITCH_REDIRECT_URI` | No | `http://localhost:8080/callback` | OAuth redirect URI for Twitch |
| `TARGET_TWITCH_STREAMER` | No | *(empty)* | Streamer whose live status pauses the rerun. If empty, live detection is disabled — ideal for pure 24/7 streams. |
| `ENABLE_KICK` | No | `false` | Enable Kick integration (optional — not required if only using Twitch) |
| `KICK_CLIENT_ID` | If Kick enabled | — | Kick application client ID |
| `KICK_CLIENT_SECRET` | If Kick enabled | — | Kick application client secret |
| `KICK_CHANNEL_ID` | No | *(auto-resolved)* | Your 24/7 Kick channel ID. Leave empty — auto-resolved during first OAuth login. |
| `KICK_REDIRECT_URI` | If Kick enabled | `http://localhost:8080/callback` | OAuth redirect URI for Kick |
| `TARGET_KICK_STREAMER` | No | *(empty)* | Kick channel slug whose live status pauses the rerun. If empty, Kick live detection is disabled. |
| `OBS_HOST` | No | `localhost` | OBS WebSocket host |
| `OBS_PORT` | No | `4455` | OBS WebSocket port |
| `OBS_PASSWORD` | Yes | — | OBS WebSocket password |
| `SCENE_PAUSE` | No | `OSR Pause screen` | OBS scene shown when target streamer is live (pauses 24/7 content) |
| `SCENE_STREAM` | No | `OSR Stream` | OBS scene for normal 24/7 playback |
| `SCENE_ROTATION_SCREEN` | No | `OSR Rotation screen` | OBS scene shown during rotation transitions |
| `VLC_SOURCE_NAME` | No | `OSR Playlist` | Name of the VLC media source in OBS |
| `DISCORD_WEBHOOK_URL` | No | — | Discord webhook for notifications |
| `WEB_DASHBOARD_URL` | No | — | URL of the OpenStreamRotator Web Dashboard backend (e.g. `https://your-domain:8000`) |
| `WEB_DASHBOARD_API_KEY` | No | — | API key from the web dashboard Team page (generated when you register an instance) |
| `VIDEO_FOLDER` | No | `content/live/` | Path to the live playback folder (VLC reads from here) |
| `NEXT_ROTATION_FOLDER` | No | `content/pending/` | Path to the pending/download folder |
| `BROADCASTER_ID` | No | — | Twitch broadcaster ID (auto-resolved from `TWITCH_USER_LOGIN` if empty) |
| `OBS_PATH` | No | *(auto-detected)* | Full path to `obs64.exe`. Auto-detected from standard install locations if not set. |

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
  ]
}
```

| Field | Description |
|---|---|
| `name` | Display name used in stream titles and logs |
| `url` | YouTube playlist URL |
| `category` | Platform category set when videos from this playlist are playing (e.g. `"Just Chatting"`, `"Final Fantasy XIV"`). Falls back to the playlist name if not set. |
| `enabled` | Whether this playlist is available for rotation selection |
| `priority` | Selection priority (lower = higher priority among equally-old playlists) |

### Settings (`config/settings.json`)

All settings in this file are **hot-swappable** — you can edit and save while the program is running and changes take effect within seconds. See [Hot-Swappable Configuration](#hot-swappable-configuration) below for details and examples.

```json
{
  "min_playlists_per_rotation": 2,
  "max_playlists_per_rotation": 2,
  "download_retry_attempts": 5,
  "stream_title_template": "24/7 Reruns | {GAMES} | ",
  "yt_dlp_verbose": false,
  "notify_video_transitions": false,
  "ignore_streamer": false,
  "yt_dlp_use_cookies": false,
  "yt_dlp_browser_for_cookies": "firefox"
}
```

| Field | Description |
|---|---|
| `min_playlists_per_rotation` | Minimum playlists selected per rotation |
| `max_playlists_per_rotation` | Maximum playlists selected per rotation |
| `download_retry_attempts` | Number of retry attempts for failed downloads |
| `stream_title_template` | Title template. `{GAMES}` is replaced with playlist names joined by ` \| ` |
| `yt_dlp_verbose` | Enable verbose yt-dlp output in logs |
| `notify_video_transitions` | Send a Discord notification on every video transition (default: `false`, can be noisy with short videos) |
| `ignore_streamer` | Prevents the target streamer going live from pausing the 24/7 stream (default: `false`) |
| `yt_dlp_use_cookies` | Use browser cookies for age-restricted videos (default: `false`). Toggle mid-download-retry to recover from 403s. |
| `yt_dlp_browser_for_cookies` | Browser to extract cookies from: `chrome`, `firefox`, `brave`, `edge`, etc. (default: `firefox`) |

### Hot-Swappable Configuration

Both `config/settings.json` and `config/playlists.json` can be changed **while the program is running** — no restart required. The system re-reads both files every loop iteration (once per second), so your changes take effect almost immediately.

`.env` values are **not** hot-swappable. Environment variables are loaded once at process startup. Changing `.env` requires a full restart.

| File | Hot-swappable | What lives here |
|---|---|---|
| `config/settings.json` | Yes | All runtime behavior settings |
| `config/playlists.json` | Yes | Playlist definitions (enable/disable, priorities, URLs) |
| `.env` | No (restart required) | Credentials, folder paths, platform toggles |

**Example — Recovering from YouTube 403 errors mid-download:**

If downloads start failing with 403 (Forbidden) errors — common when YouTube detects automated requests — you can enable cookie-based authentication on the fly without stopping the program:

1. Open `config/settings.json` in any text editor.
2. Change `"yt_dlp_use_cookies"` from `false` to `true`:
   ```json
   {
     "yt_dlp_use_cookies": true,
     "yt_dlp_browser_for_cookies": "firefox"
   }
   ```
3. Save the file. The program re-reads it before each download attempt, so the very next retry will use cookies from your browser session (where you're logged into YouTube).

This works because the download retry loop reads cookie settings fresh on every attempt. If the program is in the middle of retrying a failed download (with exponential backoff between attempts), your change will be picked up on the next retry — no content interruption.

> **Tip:** Make sure you're logged into YouTube in the browser specified by `yt_dlp_browser_for_cookies` and that the browser is closed (some browsers lock their cookie database while running).

**Example — Ignoring streamer live status:**

If the target streamer goes live and you want to keep your 24/7 stream running instead of pausing:

1. Set `"ignore_streamer": true` in `config/settings.json` and save.
2. The program ignores live-status checks until you set it back to `false`.

**Example — Adjusting rotation size:**

Want more variety in the next rotation? Change `max_playlists_per_rotation` from `2` to `4` in `config/settings.json` and save. The *next rotation* will pick up to 4 playlists.

**Example — Disabling a playlist mid-run:**

Want to stop a playlist from being selected in future rotations? Open `config/playlists.json`, set `"enabled": false` on that playlist, and save. It won't be picked for the next rotation.

## OBS Setup

On first startup, the system automatically creates any missing scenes and sources in OBS:

- **`OSR Stream`** — with a VLC Video Source named `Playlist` pointing to your video folder
- **`OSR Pause screen`** — with an Image source pointing to `content/pause/default.png`
- **`OSR Rotation screen`** — with an Image source pointing to `content/rotation/default.png`

All sources are automatically sized to fill the canvas. You can replace the default images with your own, add overlays, or customize the scenes in OBS as needed.

### Scenes

1. **`OSR Stream`** (default name, configurable via `SCENE_STREAM`) — The main playback scene. Contains your VLC media source and any overlays. This is what viewers see during normal 24/7 operation.
2. **`OSR Pause screen`** (default name, configurable via `SCENE_PAUSE`) — Shown when the target streamer goes live. Typically a static image telling viewers the main stream is live.
3. **`OSR Rotation screen`** (default name, configurable via `SCENE_ROTATION_SCREEN`) — Brief transition scene shown while content folders are being swapped. Can be a loading animation or static image.

### VLC Media Source

The VLC source is created automatically inside the `OSR Stream` scene. If you need to configure it manually, add a **VLC Video Source** (not a regular Media Source) named **`OSR Playlist`** (configurable via `VLC_SOURCE_NAME`) to your stream scene with these settings:

- **Playlist directory**: Point it to your `VIDEO_FOLDER` path
- **Loop**: Enabled (the system handles advancement by deleting played files; loop ensures VLC keeps playing)
- **Shuffle**: Disabled (the system controls ordering via filename prefixes)

> The VLC source plugin for OBS must be installed separately if not already present.

## Kick Setup

When Kick integration is enabled, the program uses OAuth for authentication:

1. On first startup, a browser window opens automatically to the Kick authorization page.
2. Log in and authorize the application.
3. You'll be redirected to a URL containing an authorization code (the page won't load — that's expected).
4. Copy the full redirect URL from your browser's address bar and paste it into the terminal.
5. Tokens are saved in `core/kick_tokens.db` and refreshed automatically for subsequent runs.

## Twitch Setup

Twitch uses the OAuth Authorization Code flow for channel updates (title and category). Live status checking uses a separate app token that's generated automatically.

1. Create a Twitch application at [dev.twitch.tv/console](https://dev.twitch.tv/console).
2. Set **Client Type** to **Confidential**.
3. Add your `TWITCH_REDIRECT_URI` (default: `http://localhost:8080/callback`) as an **OAuth Redirect URL**.
4. Copy the Client ID and Client Secret to your `.env` file.
5. On first startup, a browser window opens to the Twitch authorization page.
6. Log in and click **Authorize**.
7. You'll be redirected to a URL like `http://localhost:8080/callback?code=abc123...` (the page won't load — that's expected).
8. Copy the full redirect URL from your browser's address bar and paste it into the terminal.
9. Tokens are saved in `core/twitch_tokens.db` and auto-refresh when they expire. You only need to do this once.

If you change your Twitch password or disconnect the app, the refresh token becomes invalid and you'll be prompted to re-authorize on next startup.

## Running

Use the run script (recommended) or start manually:

**Windows:**
```bash
run.bat
```

**Linux/Mac:**
```bash
./run.sh
```

**Or manually:**
```bash
python main.py
```

The system will:
1. Connect to OBS via WebSocket
2. Create any missing scenes and sources in OBS (or verify existing ones)
3. Sync playlist configuration to the database
4. Resume any interrupted session or start a new rotation
5. Begin the main automation loop

### What Happens Each Rotation

1. Playlists are selected (least-recently-played first, excluding currently playing and currently downloading playlists)
2. Videos are downloaded via yt-dlp into the pending folder
3. When downloads complete, OBS switches to the Rotation screen scene
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

- If videos remain in the live folder, playback resumes from the last saved position (cursor is saved every second)
- The stream title and category are restored automatically
- If a temp playback session was active, it's restored
- If mid-download, downloads resume (yt-dlp handles partial file resumption)
- A Discord notification is sent with the resumed session ID and playback position

### OBS Freeze Recovery

Separate from crash recovery, the system detects frozen OBS processes. OBS may appear running but stop rendering frames (GPU hang, driver issue, etc.). The freeze monitor:

1. Polls OBS render frame count every 20 seconds via WebSocket
2. If frames don't advance for 3 consecutive checks (~60 seconds), OBS is declared frozen
3. The current stream state (streaming status, playback position) is captured
4. OBS is killed via `taskkill` (Windows) or `pkill` (Linux)
5. OBS crash sentinel files are cleared to prevent the Safe Mode dialog on relaunch
6. OBS is relaunched with `--minimize-to-tray` and `--disable-missing-files-check`
7. WebSocket reconnection is attempted automatically
8. Streaming is resumed if it was active before the freeze

Freeze recovery is retried on future freezes as long as the previous recovery succeeded. If a recovery fails, further automatic restarts are blocked to avoid restart loops — a notification is sent and manual intervention is required.

### Skipping Videos

You can skip videos directly in OBS by advancing the VLC source. The file lock monitor will detect the transition naturally — the skipped video's lock is released, it gets deleted, and the next video is tracked.

### Graceful Shutdown

Press `Ctrl+C` to stop. The system handles the interrupt cleanly and shuts down.

## Discord Notifications

If `DISCORD_WEBHOOK_URL` is set, the system sends notifications for:

- **Automation Started / Shutting Down** — program lifecycle events
- **Now Playing** — playlist names after a content switch completes
- **Session Resumed** — crash recovery with video name and timestamp
- **Temp Playback Activated / Complete** — long download handling
- **Video Transition** — per-video notifications (opt-in via `notify_video_transitions` in `settings.json`)
- **Rotation downloads** — started, ready, errors, warnings
- **Stream metadata failures** — title/category update errors
- **Streamer live/offline** — target streamer status changes
- **Automation errors** — unexpected exceptions

## Live Detection

The system polls configured platforms every 60 seconds to check if the target streamer is live:

- **Twitch** — set `TARGET_TWITCH_STREAMER` to a Twitch username (requires `TWITCH_CLIENT_ID` / `TWITCH_CLIENT_SECRET`)
- **Kick** — set `TARGET_KICK_STREAMER` to a Kick channel slug (requires `KICK_CLIENT_ID` / `KICK_CLIENT_SECRET`)

If both are configured, either platform being live triggers the pause. If neither is set, live detection is skipped entirely — ideal for pure 24/7 streams (e.g., music or ambient content) that should never pause.

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

This deletes the SQLite database and all downloaded videos from both the live and pending folders (paths are read from `VIDEO_FOLDER` / `NEXT_ROTATION_FOLDER` env vars).

## Project Structure

```
OpenStreamRotator/
├── main.py                          # Entry point
├── config/
│   ├── config_manager.py            # Loads playlists.json and settings.json
│   ├── constants.py                 # Application constants
│   ├── playlists.json               # Playlist definitions
│   └── settings.json                # Runtime settings (hot-swappable)
├── controllers/
│   ├── automation_controller.py     # Main orchestration loop
│   └── obs_controller.py            # OBS WebSocket interface
├── core/
│   ├── database.py                  # SQLite session, playlist, and playback tracking
│   └── video_registration_queue.py  # Async queue for video file registration
├── handlers/
│   ├── content_switch_handler.py    # OBS scene transitions during rotation
│   ├── dashboard_handler.py         # Web dashboard command handler
│   └── temp_playback_handler.py     # Temp playback during long downloads
├── integrations/
│   └── platforms/
│       ├── base/stream_platform.py  # Platform interface
│       ├── kick.py                  # Kick API integration
│       └── twitch.py                # Twitch API integration
├── monitors/
│   └── obs_freeze_monitor.py     # OBS freeze detection and recovery
├── managers/
│   ├── download_manager.py          # Download orchestration and retry logic
│   ├── obs_connection_manager.py    # OBS WebSocket connection lifecycle
│   ├── platform_manager.py          # Multi-platform broadcast orchestration
│   ├── playlist_manager.py          # Folder management and video renaming
│   ├── prepared_rotation_manager.py # Prepared rotation scheduling and execution
│   ├── rotation_manager.py          # Rotation lifecycle management
│   └── stream_manager.py            # Stream title and category updates
├── playback/
│   └── file_lock_monitor.py         # File-lock-based video transition detection
├── services/
│   ├── kick_live_checker.py         # Kick live status polling
│   ├── notification_service.py      # Discord webhook notifications
│   ├── twitch_live_checker.py       # Twitch live status polling
│   └── web_dashboard_client.py      # WebSocket client for the web dashboard
├── utils/
│   ├── playlist_selector.py         # Rotation playlist selection logic
│   ├── video_downloader.py          # yt-dlp download wrapper
│   ├── video_processor.py           # Video file processing utilities
│   └── video_utils.py               # Video duration and validation helpers
└── content/
    ├── live/                        # Current playback folder (VLC reads from here)
    ├── pause/                       # Pause screen assets
    ├── pending/                     # Next rotation downloads
    ├── prepared/                    # Prepared rotation content staging
    └── rotation/                    # Rotation screen assets
```

## Web Dashboard

OpenStreamRotator has an optional web dashboard for remote monitoring and control. See [OpenStreamRotatorWeb](https://github.com/theimperious1/OpenStreamRotatorWeb) for setup instructions.

To connect an OSR instance to the dashboard, set `WEB_DASHBOARD_URL` and `WEB_DASHBOARD_API_KEY` in your `.env` file. The API key is generated when you register an instance on the Team page of the web dashboard.

## License

See [LICENSE](LICENSE) for details.