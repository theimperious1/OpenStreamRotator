# 24/7 Stream Automation System

Fully automated content rotation system for 24/7 Twitch streams with OBS integration.

## Features

- ✅ **Automatic Content Rotation**: Rotates playlists every 12 hours of playback
- ✅ **Smart Playlist Selection**: Prioritizes content that hasn't been played recently
- ✅ **Seamless Downloads**: Downloads next rotation in background using yt-dlp
- ✅ **OBS Integration**: Automatically updates VLC sources and switches scenes
- ✅ **Twitch Integration**: Updates stream title based on current playlists
- ✅ **Manual Override**: Take control when needed (e.g., when Asmon raids)
- ✅ **Discord Notifications**: Get notified of rotation events and errors
- ✅ **Playback Time Tracking**: Only counts actual streaming time (excludes when Asmon is live)

## Prerequisites

1. **Python 3.10+**
2. **OBS Studio 28+** (with WebSocket enabled)
3. **yt-dlp** installed and in PATH
4. **VLC Media Player** installed
5. **Firefox** (for cookies to bypass YouTube rate limits)

## Installation

### 1. Install Dependencies

```bash
pip install obsws-python python-dotenv requests
```

### 2. Install yt-dlp

```bash
pip install yt-dlp
```

Or download from: https://github.com/yt-dlp/yt-dlp

### 3. Configure OBS

1. Open OBS Studio
2. Go to **Tools → WebSocket Server Settings**
3. Enable WebSocket server
4. Set a password
5. Note the port (default: 4455)

### 4. Create Required Scenes

Create these scenes in OBS:
- **"Stream"** - Main scene with VLC Video Source
- **"Pause screen"** - Shown when Asmongold is live
- **"content-switch"** - Shown during content rotation

### 5. Add VLC Video Source

1. In the "Stream" scene, add a **VLC Video Source**
2. Name it "Playlist" (or customize `VLC_SOURCE_NAME` in `.env`)
3. The script will manage the playlist automatically

### 6. Set Up Configuration Files

Create `.env` file:
```env
OBS_HOST=127.0.0.1
OBS_PORT=4455
OBS_PASSWORD=your_obs_password
SCENE_LIVE=Pause screen
SCENE_OFFLINE=Stream
SCENE_CONTENT_SWITCH=content-switch
VLC_SOURCE_NAME=Playlist

TWITCH_CLIENT_ID=your_client_id
TWITCH_CLIENT_SECRET=your_client_secret
TWITCH_USER_LOGIN=your_24_7_channel_username

DISCORD_WEBHOOK_URL=your_discord_webhook_url
```

Create `playlists.json`:
```json
{
  "playlists": [
    {
      "name": "Classic WoW",
      "url": "https://www.youtube.com/playlist?list=YOUR_PLAYLIST_ID",
      "enabled": true,
      "priority": 1
    }
  ],
  "settings": {
    "rotation_hours": 12,
    "video_folder": "C:/stream_videos/",
    "next_rotation_folder": "C:/stream_videos_next/",
    "min_playlists_per_rotation": 2,
    "max_playlists_per_rotation": 4
  }
}
```

### 7. Create Video Folders

```bash
mkdir C:\stream_videos
mkdir C:\stream_videos_next
```

## Usage

### Starting the System

```bash
python main.py
```

The system will:
1. Connect to OBS
2. Load playlists from `playlists.json`
3. Download initial content
4. Start streaming
5. Monitor Asmongold's stream status
6. Rotate content every 12 hours of playback

### Adding New Playlists

1. Edit `playlists.json`
2. Add new playlist entry
3. The script automatically detects changes every 60 seconds

```json
{
  "name": "New Game",
  "url": "https://www.youtube.com/playlist?list=...",
  "enabled": true,
  "priority": 1
}
```

### Manual Override (Taking Control)

When Asmongold raids the channel or you want specific content:

1. Edit `manual_override.json`:
```json
{
  "override_active": true,
  "selected_playlists": ["Classic WoW", "Diablo 4"],
  "trigger_now": true
}
```

2. The script will:
   - Download selected playlists
   - Switch to "content-switch" scene
   - Replace current content
   - Update stream title
   - Resume streaming

3. After completion, it resets to automatic mode

### Disabling a Playlist Temporarily

In `playlists.json`, set `"enabled": false`:

```json
{
  "name": "Boring Game",
  "url": "...",
  "enabled": false,
  "priority": 1
}
```

## How It Works

### Automatic Mode

1. **Selection**: Picks 2-4 playlists that haven't been played recently
2. **Download**: Downloads to `next_rotation_folder` in background
3. **Tracking**: Counts playback time (only when Asmon is offline)
4. **Rotation**: After 12 hours of playback:
   - Switches to "content-switch" scene
   - Deletes old videos
   - Moves new videos to main folder
   - Updates OBS VLC source
   - Updates Twitch title
   - Switches back to "Stream" scene

### Scene Switching

- **Asmon goes live** → Switch to "Pause screen"
- **Asmon goes offline** → Switch to "Stream" (resume videos)
- **Content rotation** → Switch to "content-switch" → back to "Stream"

### Playback Time Tracking

Only counts time when:
- Currently on "Stream" scene
- Asmongold is NOT live
- System is not rotating content

This ensures exactly 12 hours of actual viewer-facing content per rotation.

## Database

The system uses SQLite (`stream_data.db`) to track:
- Playlist play history
- Video metadata
- Rotation sessions
- Playback time

You can query this for analytics:
```sql
SELECT name, play_count, last_played 
FROM playlists 
ORDER BY play_count DESC;
```

## Troubleshooting

### Downloads Failing

**Problem**: yt-dlp rate limited by YouTube

**Solution**: 
- Ensure Firefox is installed (for cookies)
- Download fewer playlists per rotation
- Increase `download_retry_attempts` in config

### OBS Connection Failed

**Problem**: Cannot connect to OBS WebSocket

**Solution**:
- Check OBS is running
- Verify WebSocket is enabled
- Confirm port and password in `.env`

### VLC Source Not Updating

**Problem**: Videos not showing in OBS

**Solution**:
- Ensure VLC Media Player is installed
- Check `VLC_SOURCE_NAME` matches your source name
- Verify video files exist in `video_folder`

### Content Not Rotating

**Problem**: Still playing old content after 12 hours

**Solution**:
- Check logs: `automation.log`
- Verify `rotation_hours` in config
- Ensure playback time is being tracked (check Discord notifications)

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
  "stream_title_template": "24/7 {GAMES} | !discord !youtube"
}
```

### Rotation Timing

Change rotation interval:

```json
{
  "rotation_hours": 8
}
```

## Monitoring

### Logs

Check `automation.log` for detailed operation logs:
```bash
tail -f automation.log
```

### Discord Notifications

Receive notifications for:
- ✅ Content rotation started
- ✅ Content rotation completed
- ✅ Asmongold live/offline status
- ❌ Download failures
- ❌ OBS connection errors
- ❌ Critical errors

## File Structure

```
project/
├── main.py                      # Main orchestrator
├── database.py                  # Database operations
├── config_manager.py            # Config file handling
├── playlist_manager.py          # Download & selection logic
├── stream_updater.py            # OBS & Twitch API
├── playlists.json              # Playlist configuration
├── manual_override.json        # Manual control
├── .env                        # Credentials
├── stream_data.db              # SQLite database
├── automation.log              # Application logs
└── C:/stream_videos/           # Current content
    └── C:/stream_videos_next/  # Next rotation content
```

## Credits

Built for Kryptiiq's Asmongold 24/7 channel automation.