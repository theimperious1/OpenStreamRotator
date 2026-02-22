#!/usr/bin/env bash
# ============================================================
#  OpenStreamRotator — Interactive Setup
#  Walks you through configuring your .env file step by step.
#  Nothing is saved until you confirm at the end.
# ============================================================

set -e

echo ""
echo " ======================================="
echo "  OpenStreamRotator - Interactive Setup"
echo " ======================================="
echo ""

# ----------------------------------------------------------
# Helper: prompt with default
# ----------------------------------------------------------
ask() {
    local prompt="$1"
    local default="$2"
    local result
    if [ -n "$default" ]; then
        read -rp "  $prompt [$default]: " result
        echo "${result:-$default}"
    else
        read -rp "  $prompt: " result
        echo "$result"
    fi
}

ask_yn() {
    local prompt="$1"
    local default="${2:-n}"
    local result
    if [ "$default" = "y" ]; then
        read -rp "  $prompt [Y/n]: " result
        result="${result:-y}"
    else
        read -rp "  $prompt [y/N]: " result
        result="${result:-n}"
    fi
    [[ "$result" =~ ^[Yy]$ ]]
}

# ----------------------------------------------------------
# Check if .env already exists
# ----------------------------------------------------------
if [ -f ".env" ]; then
    echo " A .env file already exists."
    if ! ask_yn "Overwrite it? Existing values will be lost."; then
        echo ""
        echo " Setup cancelled. Your existing .env was not modified."
        echo ""
        exit 0
    fi
    echo ""
fi

# ----------------------------------------------------------
# Defaults
# ----------------------------------------------------------
VAL_ENABLE_TWITCH="false"
VAL_TWITCH_CLIENT_ID=""
VAL_TWITCH_CLIENT_SECRET=""
VAL_TWITCH_USER_LOGIN=""
VAL_BROADCASTER_ID=""
VAL_TWITCH_REDIRECT_URI="http://localhost:8080/callback"

VAL_ENABLE_KICK="false"
VAL_KICK_CLIENT_ID=""
VAL_KICK_CLIENT_SECRET=""
VAL_KICK_CHANNEL_ID=""
VAL_KICK_REDIRECT_URI="http://localhost:8080/callback"

VAL_TARGET_TWITCH_STREAMER=""
VAL_TARGET_KICK_STREAMER=""

VAL_OBS_HOST="localhost"
VAL_OBS_PORT="4455"
VAL_OBS_PASSWORD=""
VAL_SCENE_PAUSE="OSR Pause screen"
VAL_SCENE_STREAM="OSR Stream"
VAL_SCENE_ROTATION_SCREEN="OSR Rotation screen"
VAL_VLC_SOURCE_NAME="OSR Playlist"
VAL_OBS_PATH=""

VAL_DISCORD_WEBHOOK_URL=""

VAL_WEB_DASHBOARD_URL=""
VAL_WEB_DASHBOARD_API_KEY=""

WANT_TWITCH="n"
WANT_KICK="n"

# ===========================================================
#  1. OBS (required)
# ===========================================================
echo " --- OBS Configuration (required) ---"
echo ""
echo " OSR connects to OBS via WebSocket."
echo " Open OBS > Tools > WebSocket Server Settings to find these values."
echo ""
VAL_OBS_PASSWORD=$(ask "OBS WebSocket password")
echo ""
VAL_OBS_HOST=$(ask "OBS WebSocket host" "localhost")
input=$(ask "OBS WebSocket port" "4455")
VAL_OBS_PORT="$input"
echo ""

# ===========================================================
#  2. Twitch
# ===========================================================
echo " --- Twitch Integration ---"
echo ""
if ask_yn "Do you stream on Twitch?"; then
    WANT_TWITCH="y"
    VAL_ENABLE_TWITCH="true"
    echo ""
    echo " OSR can update your Twitch stream title and category automatically."
    echo " You need a Twitch application for this:"
    echo "   1. Go to https://dev.twitch.tv/console/apps"
    echo "   2. Create or select an application"
    echo "   3. Copy the Client ID and generate a Client Secret"
    echo ""
    VAL_TWITCH_CLIENT_ID=$(ask "Twitch Client ID")
    VAL_TWITCH_CLIENT_SECRET=$(ask "Twitch Client Secret")
    VAL_TWITCH_USER_LOGIN=$(ask "Your Twitch channel name (lowercase)")
    echo ""
fi
echo ""

# ===========================================================
#  3. Kick
# ===========================================================
echo " --- Kick Integration ---"
echo ""
if ask_yn "Do you stream on Kick?"; then
    WANT_KICK="y"
    VAL_ENABLE_KICK="true"
    echo ""
    echo " OSR can update your Kick stream title and category automatically."
    echo " You need a Kick application for this:"
    echo "   1. Go to https://kick.com/settings/developer"
    echo "   2. Create an application"
    echo "   3. Copy the Client ID and Client Secret"
    echo ""
    echo " Your Kick Channel ID will be resolved automatically during first login."
    echo ""
    VAL_KICK_CLIENT_ID=$(ask "Kick Client ID")
    VAL_KICK_CLIENT_SECRET=$(ask "Kick Client Secret")
    echo ""
fi
echo ""

# ===========================================================
#  4. Live Detection
# ===========================================================
echo " --- Live Detection ---"
echo ""
echo " OSR can pause the 24/7 stream when a specific streamer goes live."
echo " This is useful if you want to pause the 24/7 while you stream live."
echo ""
if ask_yn "Do you want live detection?"; then
    echo ""
    if [ "$WANT_TWITCH" = "y" ]; then
        VAL_TARGET_TWITCH_STREAMER=$(ask "Twitch streamer to watch (leave blank to skip)")
    fi
    if [ "$WANT_KICK" = "y" ]; then
        VAL_TARGET_KICK_STREAMER=$(ask "Kick streamer to watch (leave blank to skip)")
    fi
    if [ "$WANT_TWITCH" != "y" ] && [ "$WANT_KICK" != "y" ]; then
        echo " Note: Live detection requires Twitch or Kick to be enabled."
    fi
    echo ""
fi
echo ""

# ===========================================================
#  5. Discord Webhook
# ===========================================================
echo " --- Discord Notifications ---"
echo ""
if ask_yn "Do you have a Discord webhook URL?"; then
    VAL_DISCORD_WEBHOOK_URL=$(ask "Discord Webhook URL")
    echo ""
else
    echo ""
    echo " Discord webhooks let OSR send notifications to a Discord channel"
    echo " when events happen (stream started, rotation, errors, etc.)."
    echo ""
    if ask_yn "Would you like to set one up?"; then
        echo ""
        echo " To create a webhook:"
        echo "   1. Open Discord and go to the channel you want notifications in"
        echo "   2. Click the gear icon (Edit Channel) > Integrations > Webhooks"
        echo "   3. Click \"New Webhook\", give it a name, and click \"Copy Webhook URL\""
        echo ""
        VAL_DISCORD_WEBHOOK_URL=$(ask "Discord Webhook URL")
        echo ""
    fi
fi
echo ""

# ===========================================================
#  6. Web Dashboard
# ===========================================================
echo " --- Web Dashboard ---"
echo ""
if ask_yn "Do you have a Web Dashboard URL and API key?"; then
    VAL_WEB_DASHBOARD_URL=$(ask "Dashboard URL (e.g. https://your-domain.com)")
    VAL_WEB_DASHBOARD_API_KEY=$(ask "API Key")
    echo ""
else
    echo ""
    echo " The Web Dashboard (OpenStreamRotatorWeb) lets you monitor and control"
    echo " OSR remotely from a browser — manage playlists, queue videos, view logs,"
    echo " and more. It's a separate project you can self-host."
    echo ""
    echo " GitHub: https://github.com/theimperious1/OpenStreamRotatorWeb"
    echo ""
    if ask_yn "Would you like to set it up now?"; then
        echo ""
        echo " To connect OSR to the dashboard:"
        echo "   1. Deploy OpenStreamRotatorWeb (see the repo README)"
        echo "   2. Log in and go to the Team page"
        echo "   3. Create an OSR instance and copy the API key"
        echo ""
        VAL_WEB_DASHBOARD_URL=$(ask "Dashboard URL (e.g. https://your-domain.com)")
        VAL_WEB_DASHBOARD_API_KEY=$(ask "API Key")
        echo ""
    fi
fi
echo ""

# ===========================================================
#  7. OBS Path (optional)
# ===========================================================
echo " --- OBS Freeze Recovery ---"
echo ""
echo " OSR can detect when OBS freezes and automatically restart it."
echo " If OBS is not in your system PATH, provide the full path to the OBS binary."
echo ""
input=$(ask "Path to OBS binary (leave blank if OBS is in PATH or to skip)")
[ "$input" != "" ] && VAL_OBS_PATH="$input"
echo ""

# ===========================================================
#  Summary
# ===========================================================
echo ""
echo " ======================================="
echo "  Configuration Summary"
echo " ======================================="
echo ""
echo " OBS Password:        $VAL_OBS_PASSWORD"
[ -n "$VAL_OBS_HOST" ] && echo " OBS Host:            $VAL_OBS_HOST"
[ "$VAL_OBS_PORT" != "4455" ] && echo " OBS Port:            $VAL_OBS_PORT"
echo " Twitch:              $VAL_ENABLE_TWITCH"
if [ "$VAL_ENABLE_TWITCH" = "true" ]; then
    echo "   Client ID:         $VAL_TWITCH_CLIENT_ID"
    echo "   Channel:           $VAL_TWITCH_USER_LOGIN"
fi
echo " Kick:                $VAL_ENABLE_KICK"
if [ "$VAL_ENABLE_KICK" = "true" ]; then
    echo "   Client ID:         $VAL_KICK_CLIENT_ID"
fi
[ -n "$VAL_TARGET_TWITCH_STREAMER" ] && echo " Live Watch (Twitch): $VAL_TARGET_TWITCH_STREAMER"
[ -n "$VAL_TARGET_KICK_STREAMER" ] && echo " Live Watch (Kick):   $VAL_TARGET_KICK_STREAMER"
[ -n "$VAL_DISCORD_WEBHOOK_URL" ] && echo " Discord Webhook:     (set)"
[ -n "$VAL_WEB_DASHBOARD_URL" ] && echo " Dashboard URL:       $VAL_WEB_DASHBOARD_URL"
[ -n "$VAL_OBS_PATH" ] && echo " OBS Path:            $VAL_OBS_PATH"
echo ""

if ! ask_yn "Save this configuration to .env?" "y"; then
    echo ""
    echo " Setup cancelled. Nothing was saved."
    echo ""
    exit 0
fi

# ===========================================================
#  Write .env
# ===========================================================
cat > .env << ENVEOF
# Twitch
ENABLE_TWITCH=$VAL_ENABLE_TWITCH
TWITCH_CLIENT_ID=$VAL_TWITCH_CLIENT_ID
TWITCH_CLIENT_SECRET=$VAL_TWITCH_CLIENT_SECRET
TWITCH_USER_LOGIN=$VAL_TWITCH_USER_LOGIN
BROADCASTER_ID=$VAL_BROADCASTER_ID
TWITCH_REDIRECT_URI=$VAL_TWITCH_REDIRECT_URI

# Kick
ENABLE_KICK=$VAL_ENABLE_KICK
KICK_CLIENT_ID=$VAL_KICK_CLIENT_ID
KICK_CLIENT_SECRET=$VAL_KICK_CLIENT_SECRET
KICK_CHANNEL_ID=$VAL_KICK_CHANNEL_ID
KICK_REDIRECT_URI=$VAL_KICK_REDIRECT_URI

# Live Detection
TARGET_TWITCH_STREAMER=$VAL_TARGET_TWITCH_STREAMER
TARGET_KICK_STREAMER=$VAL_TARGET_KICK_STREAMER

# OBS
OBS_HOST=$VAL_OBS_HOST
OBS_PORT=$VAL_OBS_PORT
OBS_PASSWORD=$VAL_OBS_PASSWORD
SCENE_PAUSE=$VAL_SCENE_PAUSE
SCENE_STREAM=$VAL_SCENE_STREAM
SCENE_ROTATION_SCREEN=$VAL_SCENE_ROTATION_SCREEN
VLC_SOURCE_NAME=$VAL_VLC_SOURCE_NAME
OBS_PATH=$VAL_OBS_PATH

# Discord
DISCORD_WEBHOOK_URL=$VAL_DISCORD_WEBHOOK_URL

# Web Dashboard
WEB_DASHBOARD_URL=$VAL_WEB_DASHBOARD_URL
WEB_DASHBOARD_API_KEY=$VAL_WEB_DASHBOARD_API_KEY
ENVEOF

echo ""
echo " .env saved successfully!"
echo ""
echo " You can now start OSR:"
echo "   - From source:  python main.py"
echo "   - From exe:     ./OpenStreamRotator"
echo ""
echo " To reconfigure later, run setup.sh again."
echo ""
