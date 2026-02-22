#!/usr/bin/env bash
# ============================================================
#  OpenStreamRotator — Run
#  Starts OpenStreamRotator from source.
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo " ======================================="
echo "  OpenStreamRotator - Starting"
echo " ======================================="
echo ""

# ----------------------------------------------------------
# Pre-flight checks
# ----------------------------------------------------------
if [ ! -f ".env" ]; then
    echo " ERROR: .env file not found."
    echo " Run ./setup.sh first to configure OSR."
    echo ""
    exit 1
fi

# ----------------------------------------------------------
# Activate venv if present
# ----------------------------------------------------------
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

# ----------------------------------------------------------
# Check dependencies
# ----------------------------------------------------------
if ! python3 -c "import aiohttp" 2>/dev/null; then
    echo " Installing dependencies..."
    pip install -r requirements.txt --quiet
    echo ""
fi

# ----------------------------------------------------------
# Start OSR
# ----------------------------------------------------------
echo " Starting OpenStreamRotator..."
echo " Press Ctrl+C to stop."
echo ""
python3 main.py
