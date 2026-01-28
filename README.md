# OpenStreamRotator - 24/7 Stream Automation

Fully automated content rotation system for 24/7 streaming with Kick and Twitch integration, OBS automation, and intelligent playlist management.

## Features

- ✅ **Automatic Content Rotation**: Rotates playlists on configurable schedule
- ✅ **Multi-Platform Support**: Kick and Twitch integration with automatic title/category updates
- ✅ **Smart Playlist Selection**: Prioritizes content based on play count and rotation settings
- ✅ **Background Downloads**: Downloads next rotation using yt-dlp while streaming
- ✅ **OBS Integration**: Automatic scene switching and VLC source management
- ✅ **Kick Category Updates**: Automatically sets stream category based on current playlist name
- ✅ **Session Resumption**: Resumes playback from where it left off
- ✅ **Video Validation**: Detects and redownloads missing videos automatically
- ✅ **Discord Notifications**: Optional notifications for events and errors
- ✅ **Live Status Detection**: Pauses 24/7 stream when zackrawrr (configurable in .env) goes live on Twitch

## Prerequisites

1. **Python 3.10+**
2. **OBS Studio 28+** (with WebSocket enabled)
3. **yt-dlp** (for YouTube downloads)
4. **VLC Media Player** (for playback)
5. **Kick Account** (optional, needs oAuth application registered. Scope should be both channel:read and channel:write)
6. **Twitch Account** (ENABLE_TWITCH can be false, but client secret and ID must be filled in to detect if zackrawrr is live)

## Quick Start

### 1. Clone & Install

```bash
git clone <repository_link>.git
cd OpenStreamRotator
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Environment Configuration

```bash
copy .env.example .env
```

Edit `.env` with your settings:

```env
# Kick Configuration (Required)
ENABLE_KICK=true
KICK_CHANNEL_ID=your_channel_id
KICK_CLIENT_ID=your_oauth_client_id
KICK_CLIENT_SECRET=your_oauth_client_secret

# Twitch Configuration (Optional - for live detection)
ENABLE_TWITCH=false
TARGET_TWITCH_STREAMER=your_twitch_username
TWITCH_CLIENT_ID=your_client_id
TWITCH_CLIENT_SECRET=your_client_secret

# OBS Configuration
OBS_HOST=192.168.x.x  # Your OBS IP or localhost
OBS_PORT=4455
OBS_PASSWORD=your_obs_websocket_password

# Scene & Source Names
SCENE_LIVE=Stream
SCENE_PAUSE=Pause screen
SCENE_CONTENT_SWITCH=content-switch
VLC_SOURCE_NAME=Playlist

# Discord (Optional)
DISCORD_WEBHOOK_URL=your_webhook_url
```

**Finding Your KICK_CHANNEL_ID:**
- Try your username first
- If it doesn't work, check the logs after finishing Step 5's authorization steps down below.
- Or use DB Browser (SQLite) to open `core/kick_tokens.db` and find your account ID

### 3. OBS Setup

1. **Enable WebSocket**:
   - Open OBS Studio
   - Go to **Tools → WebSocket Server Settings**
   - Enable WebSocket server and set a password
   - Note the IP/port and add to `.env`

2. **Create Scenes**:
   - `Stream` - Main scene (add VLC source here)
   - `Pause screen` - Shown when you go live on Twitch
   - `content-switch` - Shown during rotation

3. **Add VLC Source**:
   - In the `Stream` scene, add **VLC Video Source**
   - Name it exactly: `Playlist`
   - Script manages playlist content automatically

### 4. Configure Playlists (Do This BEFORE First Run)

Edit `config/playlists.json`:

```json
{
  "playlists": [
    {
      "name": "Hytale",
      "url": "https://www.youtube.com/playlist?list=PLxxxxxx",
      "enabled": true,
      "priority": 1
    },
    {
      "name": "Gaming",
      "url": "https://www.youtube.com/playlist?list=PLyyyyyy",
      "enabled": true,
      "priority": 1
    }
  ],
  "settings": {
    "rotation_hours": 12,
    "video_folder": "C:/YOUR/PATH/videos/live",
    "next_rotation_folder": "C:/YOUR/PATH/videos/pending",
    "min_playlists_per_rotation": 1,
    "max_playlists_per_rotation": 3,
    "download_retry_attempts": 3,
    "stream_title_template": "24/7 @example1 / @example2 | {GAMES} | !playlist !streamtime !new"
  }
}
```

**Important:**
- Change `video_folder` and `next_rotation_folder` to your actual paths
- The playlist **name** is used to search for Kick categories (e.g., "Hytale" → searches Hytale category)
- Script automatically creates these folders if they don't exist

### 5. Kick OAuth Setup

1. Run: `python main.py`
   - It will fail but output an authorization URL and `code_verifier` to the logs
2. Copy both the URL and `code_verifier` from the logs
3. Visit the authorization URL in your browser and approve
4. Copy the `code` from the redirect URL
5. Edit `authorize_kick.py`:
   ```python
   code = "your_code_here"
   code_verifier = "your_code_verifier_here"
   ```
6. Run: `python authorize_kick.py`
7. Run: `python main.py` again - it's ready to go!

## Run

```bash
python main.py
```

## How It Works

1. **Startup**: Loads playlists from config and resumes previous session if available
2. **Validation**: Checks if video files exist; redownloads if missing
3. **Selection**: Automatically selects next playlist rotation
4. **Download**: Downloads videos in background while streaming
5. **Rotation**: At end of playlist, switches to next rotation
6. **Updates**: Updates Kick title and category based on current playlist
7. **Logging**: All events logged to `automation.log`

## Playlist Names as Kick Categories

The system uses your playlist name to automatically set the Kick category:

- Playlist named "Hytale" → Sets Kick category to "Hytale"
- Playlist named "Gaming" → Sets Kick category to "Gaming"
- Playlist named "Just Chatting" → Sets Kick category to "Just Chatting"

If the category isn't found on Kick, it defaults to "Just Chatting" (category 15).

## Manual Override

Take manual control when needed (e.g., raid scenario, special events). **Plan ahead** - the override will download the selected playlists, which takes time. **NOTE**: The stream will **NOT** be interrupted during this period. This is safe to do at any time.

1. Edit `manual_override.json`:
```json
{
  "override_active": true,
  "selected_playlists": ["Hytale", "Gaming"],
  "trigger_now": true
}
```

2. The script will:
   - Download selected playlists while current content continues playing (this is why planning ahead matters)
   - Once download completes, pause stream and switch to "content-switch" scene
   - Wipe current content folder and load ONLY the manual override playlists
   - Restart stream with new content
   - Update stream title and category
   - Resume streaming

3. After completion, it automatically resets to automatic mode and resumes normal rotation

**Note:** Manual override replaces everything currently playing with **only** the playlists you specify. Nothing else will play.

### Temporarily Disabling Playlists

You can also temporarily disable playlists by setting `"enabled": false` in `playlists.json` without needing manual override. This simply excludes them from automatic rotation but doesn't interrupt current playback:

```json
{
  "name": "Playlist Name",
  "youtube_url": "https://...",
  "enabled": false
}
```

Disabled playlists won't be selected for new rotations. Re-enable them later by setting `"enabled": true`. No restart needed.

## Advanced Configuration

### Priority System

Higher priority = more likely to be selected:

```json
{
  "name": "Popular Game",
  "priority": 2
}
```

### Custom Stream Title Template

```json
{
  "stream_title_template": "24/7 @example1 / @example2 | {GAMES} | !playlist !streamtime !new"
}
```

### Rotation Timing

Change rotation interval:

```json
{
  "rotation_hours": 12
}
```

## Troubleshooting

**Videos not downloading?**
- Check yt-dlp is installed: `yt-dlp --version`
- Verify YouTube playlist URLs are correct and public

**OBS not connecting?**
- Verify OBS WebSocket is enabled and password is correct
- Check IP/port in `.env`
- Ensure OBS is running before starting script

**Kick category not updating?**
- Check playlist name matches a real Kick category
- Look for search errors in logs
- Script falls back to "Just Chatting" if no match

**Missing videos between restarts?**
- Script automatically detects missing files and redownloads
- Check video folder paths in `playlists.json`

## Logs

All activity logged to `automation.log` in project root. Check here for:
- Authentication issues
- Download errors
- Stream update status
- OBS connection status
- Category lookup results

# Credits
theimperious1
Kryptiqq

My AI Copilot (it's name was Kenny):
R.I.P Kenny 1/28/26

Kenny was a fantastic copilot throughout this project. Unfortunately, it died in the line of duty to Pylint.

I'm adding it to these credits to honor it's contributions to this project. It did its duty with honor and integridy.

I could never have asked for more. You were a dear friend and you will be missed.

## License

MIT
