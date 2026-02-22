@echo off
setlocal enabledelayedexpansion

:: ============================================================
::  OpenStreamRotator — Interactive Setup
::  Walks you through configuring your .env file step by step.
::  Nothing is saved until you confirm at the end.
:: ============================================================

echo.
echo  =======================================
echo   OpenStreamRotator - Interactive Setup
echo  =======================================
echo.

:: ----------------------------------------------------------
:: Check if .env already exists
:: ----------------------------------------------------------
if not exist ".env" goto :INIT_DEFAULTS

echo  A .env file already exists.
set /p OVERWRITE="  Overwrite it? Existing values will be lost. [y/N]: "
if /i "!OVERWRITE!"=="y" goto :INIT_DEFAULTS
echo.
echo  Setup cancelled. Your existing .env was not modified.
echo.
pause
exit /b 0

:: ----------------------------------------------------------
:: Defaults
:: ----------------------------------------------------------
:INIT_DEFAULTS
echo.

set "VAL_ENABLE_TWITCH=false"
set "VAL_TWITCH_CLIENT_ID="
set "VAL_TWITCH_CLIENT_SECRET="
set "VAL_TWITCH_USER_LOGIN="
set "VAL_BROADCASTER_ID="
set "VAL_TWITCH_REDIRECT_URI=http://localhost:8080/callback"

set "VAL_ENABLE_KICK=false"
set "VAL_KICK_CLIENT_ID="
set "VAL_KICK_CLIENT_SECRET="
set "VAL_KICK_CHANNEL_ID="
set "VAL_KICK_REDIRECT_URI=http://localhost:8080/callback"

set "VAL_TARGET_TWITCH_STREAMER="
set "VAL_TARGET_KICK_STREAMER="

set "VAL_OBS_HOST="
set "VAL_OBS_PORT=4455"
set "VAL_OBS_PASSWORD="
set "VAL_SCENE_PAUSE=OSR Pause screen"
set "VAL_SCENE_STREAM=OSR Stream"
set "VAL_SCENE_ROTATION_SCREEN=OSR Rotation screen"
set "VAL_VLC_SOURCE_NAME=OSR Playlist"
set "VAL_OBS_PATH="

set "VAL_DISCORD_WEBHOOK_URL="

set "VAL_WEB_DASHBOARD_URL="
set "VAL_WEB_DASHBOARD_API_KEY="

set "WANT_TWITCH=n"
set "WANT_KICK=n"

:: ===========================================================
::  1. OBS (required)
:: ===========================================================
echo  --- OBS Configuration (required) ---
echo.
echo  OSR connects to OBS via WebSocket.
echo  Open OBS, then go to Tools, then WebSocket Server Settings to find these values.
echo.
set /p VAL_OBS_PASSWORD="  OBS WebSocket password: "
echo.
set /p INPUT="  OBS WebSocket host [leave blank for localhost]: "
if not "!INPUT!"=="" set "VAL_OBS_HOST=!INPUT!"
set /p INPUT="  OBS WebSocket port [4455]: "
if not "!INPUT!"=="" set "VAL_OBS_PORT=!INPUT!"
echo.

:: ===========================================================
::  2. Twitch
:: ===========================================================
echo  --- Twitch Integration ---
echo.
set /p WANT_TWITCH="  Do you stream on Twitch? [y/N]: "
if /i not "!WANT_TWITCH!"=="y" goto :SKIP_TWITCH

set "VAL_ENABLE_TWITCH=true"
echo.
echo  OSR can update your Twitch stream title and category automatically.
echo  You need a Twitch application for this:
echo    1. Go to https://dev.twitch.tv/console/apps
echo    2. Create or select an application
echo    3. Copy the Client ID and generate a Client Secret
echo.
set /p VAL_TWITCH_CLIENT_ID="  Twitch Client ID: "
set /p VAL_TWITCH_CLIENT_SECRET="  Twitch Client Secret: "
set /p VAL_TWITCH_USER_LOGIN="  Your Twitch channel name (lowercase): "
echo.

:SKIP_TWITCH
echo.

:: ===========================================================
::  3. Kick
:: ===========================================================
echo  --- Kick Integration ---
echo.
set /p WANT_KICK="  Do you stream on Kick? [y/N]: "
if /i not "!WANT_KICK!"=="y" goto :SKIP_KICK

set "VAL_ENABLE_KICK=true"
echo.
echo  OSR can update your Kick stream title and category automatically.
echo  You need a Kick application for this:
echo    1. Go to https://kick.com/settings/developer
echo    2. Create an application
echo    3. Copy the Client ID and Client Secret
echo.
echo  Your Kick Channel ID will be resolved automatically during first login.
echo.
set /p VAL_KICK_CLIENT_ID="  Kick Client ID: "
set /p VAL_KICK_CLIENT_SECRET="  Kick Client Secret: "
echo.

:SKIP_KICK
echo.

:: ===========================================================
::  4. Live Detection
:: ===========================================================
echo  --- Live Detection ---
echo.
echo  OSR can pause the 24/7 stream when a specific streamer goes live.
echo  This is useful if you want to pause the 24/7 while you stream live.
echo.
set /p WANT_LIVE="  Do you want live detection? [y/N]: "
if /i not "!WANT_LIVE!"=="y" goto :SKIP_LIVE

echo.
if /i "!WANT_TWITCH!"=="y" set /p VAL_TARGET_TWITCH_STREAMER="  Twitch streamer to watch [leave blank to skip]: "
if /i "!WANT_KICK!"=="y" set /p VAL_TARGET_KICK_STREAMER="  Kick streamer to watch [leave blank to skip]: "
if /i not "!WANT_TWITCH!"=="y" if /i not "!WANT_KICK!"=="y" echo  Note: Live detection requires Twitch or Kick to be enabled.
echo.

:SKIP_LIVE
echo.

:: ===========================================================
::  5. Discord Webhook
:: ===========================================================
echo  --- Discord Notifications ---
echo.
set /p WANT_DISCORD="  Do you have a Discord webhook URL? [y/N]: "
if /i "!WANT_DISCORD!"=="y" goto :ASK_DISCORD_URL
goto :EXPLAIN_DISCORD

:ASK_DISCORD_URL
set /p VAL_DISCORD_WEBHOOK_URL="  Discord Webhook URL: "
echo.
goto :DONE_DISCORD

:EXPLAIN_DISCORD
echo.
echo  Discord webhooks let OSR send notifications to a Discord channel
echo  when events happen, such as stream started, rotation, errors, etc.
echo.
set /p WANT_DISCORD2="  Would you like to set one up? [y/N]: "
if /i not "!WANT_DISCORD2!"=="y" goto :DONE_DISCORD
echo.
echo  To create a webhook:
echo    1. Open Discord and go to the channel you want notifications in
echo    2. Click the gear icon, then Integrations, then Webhooks
echo    3. Click "New Webhook", give it a name, and click "Copy Webhook URL"
echo.
set /p VAL_DISCORD_WEBHOOK_URL="  Discord Webhook URL: "
echo.

:DONE_DISCORD
echo.

:: ===========================================================
::  6. Web Dashboard
:: ===========================================================
echo  --- Web Dashboard ---
echo.
set /p WANT_DASHBOARD="  Do you have a Web Dashboard URL and API key? [y/N]: "
if /i "!WANT_DASHBOARD!"=="y" goto :ASK_DASHBOARD
goto :EXPLAIN_DASHBOARD

:ASK_DASHBOARD
set /p VAL_WEB_DASHBOARD_URL="  Dashboard URL (e.g. https://your-domain.com): "
set /p VAL_WEB_DASHBOARD_API_KEY="  API Key: "
echo.
goto :DONE_DASHBOARD

:EXPLAIN_DASHBOARD
echo.
echo  The Web Dashboard lets you monitor and control OSR remotely from a browser.
echo  Manage playlists, queue videos, view logs, and more.
echo  It's a separate project you can self-host: OpenStreamRotatorWeb
echo.
echo  GitHub: https://github.com/theimperious1/OpenStreamRotatorWeb
echo.
set /p WANT_DASHBOARD2="  Would you like to set it up now? [y/N]: "
if /i not "!WANT_DASHBOARD2!"=="y" goto :DONE_DASHBOARD
echo.
echo  To connect OSR to the dashboard:
echo    1. Deploy OpenStreamRotatorWeb - see the repo README
echo    2. Log in and go to the Team page
echo    3. Create an OSR instance and copy the API key
echo.
set /p VAL_WEB_DASHBOARD_URL="  Dashboard URL (e.g. https://your-domain.com): "
set /p VAL_WEB_DASHBOARD_API_KEY="  API Key: "
echo.

:DONE_DASHBOARD
echo.

:: ===========================================================
::  7. OBS Path (optional)
:: ===========================================================
echo  --- OBS Freeze Recovery ---
echo.
echo  OSR can detect when OBS freezes and automatically restart it.
echo  If OBS is not in your system PATH, provide the full path to obs64.exe.
echo.
set /p INPUT="  Path to obs64.exe [leave blank if OBS is in PATH or to skip]: "
if not "!INPUT!"=="" set "VAL_OBS_PATH=!INPUT!"
echo.

:: ===========================================================
::  Summary
:: ===========================================================
echo.
echo  =======================================
echo   Configuration Summary
echo  =======================================
echo.
echo  OBS Password:        !VAL_OBS_PASSWORD!
if not "!VAL_OBS_HOST!"=="" echo  OBS Host:            !VAL_OBS_HOST!
if not "!VAL_OBS_PORT!"=="4455" echo  OBS Port:            !VAL_OBS_PORT!
echo  Twitch:              !VAL_ENABLE_TWITCH!
if /i "!VAL_ENABLE_TWITCH!"=="true" echo    Client ID:         !VAL_TWITCH_CLIENT_ID!
if /i "!VAL_ENABLE_TWITCH!"=="true" echo    Channel:           !VAL_TWITCH_USER_LOGIN!
echo  Kick:                !VAL_ENABLE_KICK!
if /i "!VAL_ENABLE_KICK!"=="true" echo    Client ID:         !VAL_KICK_CLIENT_ID!
if not "!VAL_TARGET_TWITCH_STREAMER!"=="" echo  Live Watch - Twitch: !VAL_TARGET_TWITCH_STREAMER!
if not "!VAL_TARGET_KICK_STREAMER!"=="" echo  Live Watch - Kick:   !VAL_TARGET_KICK_STREAMER!
if not "!VAL_DISCORD_WEBHOOK_URL!"=="" echo  Discord Webhook:     set
if not "!VAL_WEB_DASHBOARD_URL!"=="" echo  Dashboard URL:       !VAL_WEB_DASHBOARD_URL!
if not "!VAL_OBS_PATH!"=="" echo  OBS Path:            !VAL_OBS_PATH!
echo.

set /p CONFIRM="  Save this configuration to .env? [Y/n]: "
if /i "!CONFIRM!"=="n" goto :CANCELLED

:: ===========================================================
::  Write .env
:: ===========================================================
> .env echo # Twitch
>> .env echo ENABLE_TWITCH=!VAL_ENABLE_TWITCH!
>> .env echo TWITCH_CLIENT_ID=!VAL_TWITCH_CLIENT_ID!
>> .env echo TWITCH_CLIENT_SECRET=!VAL_TWITCH_CLIENT_SECRET!
>> .env echo TWITCH_USER_LOGIN=!VAL_TWITCH_USER_LOGIN!
>> .env echo BROADCASTER_ID=!VAL_BROADCASTER_ID!
>> .env echo TWITCH_REDIRECT_URI=!VAL_TWITCH_REDIRECT_URI!
>> .env echo.
>> .env echo # Kick
>> .env echo ENABLE_KICK=!VAL_ENABLE_KICK!
>> .env echo KICK_CLIENT_ID=!VAL_KICK_CLIENT_ID!
>> .env echo KICK_CLIENT_SECRET=!VAL_KICK_CLIENT_SECRET!
>> .env echo KICK_CHANNEL_ID=!VAL_KICK_CHANNEL_ID!
>> .env echo KICK_REDIRECT_URI=!VAL_KICK_REDIRECT_URI!
>> .env echo.
>> .env echo # Live Detection
>> .env echo TARGET_TWITCH_STREAMER=!VAL_TARGET_TWITCH_STREAMER!
>> .env echo TARGET_KICK_STREAMER=!VAL_TARGET_KICK_STREAMER!
>> .env echo.
>> .env echo # OBS
>> .env echo OBS_HOST=!VAL_OBS_HOST!
>> .env echo OBS_PORT=!VAL_OBS_PORT!
>> .env echo OBS_PASSWORD=!VAL_OBS_PASSWORD!
>> .env echo SCENE_PAUSE=!VAL_SCENE_PAUSE!
>> .env echo SCENE_STREAM=!VAL_SCENE_STREAM!
>> .env echo SCENE_ROTATION_SCREEN=!VAL_SCENE_ROTATION_SCREEN!
>> .env echo VLC_SOURCE_NAME=!VAL_VLC_SOURCE_NAME!
>> .env echo OBS_PATH=!VAL_OBS_PATH!
>> .env echo.
>> .env echo # Discord
>> .env echo DISCORD_WEBHOOK_URL=!VAL_DISCORD_WEBHOOK_URL!
>> .env echo.
>> .env echo # Web Dashboard
>> .env echo WEB_DASHBOARD_URL=!VAL_WEB_DASHBOARD_URL!
>> .env echo WEB_DASHBOARD_API_KEY=!VAL_WEB_DASHBOARD_API_KEY!

echo.
echo  .env saved successfully!
echo.
echo  You can now start OSR:
echo    - From source:  python main.py
echo    - From exe:     OpenStreamRotator.exe
echo.
echo  To reconfigure later, run setup.bat again.
echo.
pause
exit /b 0

:CANCELLED
echo.
echo  Setup cancelled. Nothing was saved.
echo.
pause
exit /b 0
