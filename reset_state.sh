#!/bin/bash
# Reset OSR state - delete database and all video files
# Reads video directories from config/playlists.json

echo "Resetting OpenStreamRotator state..."
echo ""

# Delete database
if [ -f "core/stream_data.db" ]; then
    echo "Deleting stream_data.db..."
    rm -f "core/stream_data.db"
    if [ -f "core/stream_data.db" ]; then
        echo "WARNING: Failed to delete stream_data.db (may be in use)"
    else
        echo "Successfully deleted stream_data.db"
    fi
else
    echo "stream_data.db not found (already deleted)"
fi

echo ""

# Read video folders from playlists.json using Python
echo "Reading configuration from playlists.json..."
VIDEO_FOLDER=$(python3 -c "import json; config = json.load(open('config/playlists.json')); print(config['settings'].get('video_folder', 'videos/live'))" 2>/dev/null || echo "videos/live")
NEXT_FOLDER=$(python3 -c "import json; config = json.load(open('config/playlists.json')); print(config['settings'].get('next_rotation_folder', 'videos/pending'))" 2>/dev/null || echo "videos/pending")

# Cleanup path formatting (remove trailing slashes)
VIDEO_FOLDER="${VIDEO_FOLDER%/}"
NEXT_FOLDER="${NEXT_FOLDER%/}"

echo "Configured video folder: $VIDEO_FOLDER"
echo "Configured next rotation folder: $NEXT_FOLDER"
echo ""

# Final confirmation before deletion
echo "WARNING: This will delete all files in the above folders and the database!"
echo ""
read -p "Type 'YES' to confirm and continue: " CONFIRM
if [ "$CONFIRM" != "YES" ]; then
    echo "Reset cancelled."
    exit 0
fi

echo ""
# Delete live videos
if [ -d "$VIDEO_FOLDER" ]; then
    echo "Deleting $VIDEO_FOLDER/*..."
    rm -rf "$VIDEO_FOLDER"/*
    echo "Cleared $VIDEO_FOLDER"
else
    echo "$VIDEO_FOLDER folder not found"
fi

echo ""

# Delete pending videos
if [ -d "$NEXT_FOLDER" ]; then
    echo "Deleting $NEXT_FOLDER/*..."
    rm -rf "$NEXT_FOLDER"/*
    echo "Cleared $NEXT_FOLDER"
else
    echo "$NEXT_FOLDER folder not found"
fi

echo ""

# Delete backup folders (if they exist)
echo "Cleaning backup folders..."

# Get parent directory of video folder for temp_pending_backup
VIDEO_PARENT=$(dirname "$VIDEO_FOLDER")

if [ -d "$VIDEO_PARENT/temp_pending_backup" ]; then
    echo "Deleting $VIDEO_PARENT/temp_pending_backup/*..."
    rm -rf "$VIDEO_PARENT/temp_pending_backup"/*
    rmdir "$VIDEO_PARENT/temp_pending_backup" 2>/dev/null || true
fi

echo ""
echo "Reset complete!"
echo "You can now run main.py to start fresh."
