# Known Quirks

## Video Playback

### Skipping to the end of a video may cause it to loop
You can skip forward through videos in OBS, but if you skip straight to the very end, VLC may loop the video instead of advancing to the next one. The OBS VLC source treats a skip-to-end differently from a natural track advance, so the playback monitor may not detect it as a transition. If this happens, just skip forward again or let it play through the loop — it will resolve on the next natural transition.

### Only certain video formats are recognized
The system only picks up files with these extensions: `.mp4`, `.mkv`, `.avi`, `.webm`, `.flv`, `.mov`. Other file types in the video folders are ignored. This is rarely an issue since yt-dlp downloads in supported formats, but manually added videos in other formats won't be played.

---

## OBS

### OBS scene names must match your configuration
The system switches between scenes by name (default: "OSR Stream", "OSR Pause screen", "OSR Rotation screen"). Missing scenes and sources are **auto-created** on first startup, but if you manually rename scenes in OBS without updating your `.env` file, scene switching will fail. Names are **case-sensitive** and must be an exact match.

### The VLC source must be a "VLC Video Source", not a "Media Source"
The OBS source used for playback must be a **VLC Video Source** (`vlc_source`), not a standard OBS Media Source. The VLC source plugin for OBS must be installed. If you use a regular Media Source, the system won't be able to control playback or detect video transitions. If scenes are auto-created by the system, this is handled for you.

### Brief rotation screen flash during content switches
When the system switches content between rotations or transitions in/out of temp playback, it briefly shows the Rotation screen scene (~1–3 seconds) while it swaps folders and reloads VLC. Viewers will see whatever image you've placed in that scene during this transition.

### OBS disconnection causes a temporary pause in automation
If OBS crashes or the WebSocket connection drops, the system automatically attempts to reconnect. During reconnection attempts, whatever was last showing in OBS continues to display, but no video transitions are tracked. Once reconnected, the system reinitializes and resumes. A Discord notification is sent when the disconnect is detected.

---

## Downloads & Configuration

### ffprobe (ffmpeg) must be installed for video duration detection
The system uses `ffprobe` to determine video durations and validate downloads. If ffmpeg is not installed or `ffprobe` is not on your system PATH, video durations will be reported as 0 and rotation timing estimates will be inaccurate. The system won't crash, but it won't know how long each rotation will last.

### Cookie extraction requires the browser to be closed
If you enable `yt_dlp_use_cookies` in settings to handle age-restricted or region-locked videos, the browser specified in `yt_dlp_browser_for_cookies` must be **fully closed** while the system is downloading. Some browsers lock their cookie database while running, preventing yt-dlp from reading it.

### `.env` changes require a full restart
`settings.json` and `playlists.json` are **hot-swappable** — changes are picked up within seconds with no restart needed. However, any changes to the `.env` file (credentials, folder paths, OBS connection details, platform toggles) require stopping and restarting the program.

### Stream titles are automatically truncated to 140 characters
Kick enforces a 140-character limit on stream titles. If your title template combined with playlist names exceeds this, playlist names are dropped from the end of the title until it fits. With many playlists in a single rotation, some names may not appear in the displayed title.

### Node.js is removed from PATH during runtime
The system automatically removes Node.js from the process PATH at startup to force yt-dlp to use Deno instead, preventing JavaScript runtime conflicts. If you rely on Node.js for other tools running alongside the program in the same process, be aware it won't be available.

---

## Temp Playback

### Temp playback activates automatically during long downloads
If the current rotation finishes playing before the next rotation's downloads are complete, the system automatically starts playing already-downloaded files from the pending folder to prevent dead air. You'll see a brief rotation screen transition, then playback resumes with whatever has been downloaded so far. The stream title updates to reflect the new content.

---

## Crash Recovery

### Playback resumes from the last saved position (within ~1 second)
If the process crashes or is killed, the system saves the current video and playback position every second. On restart, it resumes from that position. There may be up to a 1-second gap or brief repeat from the last save point. Stream title, category, and prepared playlists are also restored automatically.

