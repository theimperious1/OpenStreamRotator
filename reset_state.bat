@echo off
REM Reset OSR state - delete database and all video files
REM Reads video directories from .env (falls back to config/playlists.json, then defaults)

setlocal enabledelayedexpansion

echo Resetting OpenStreamRotator state...
echo.

REM Read video folders from .env first, fall back to playlists.json, then defaults
echo Reading configuration...
set "VIDEO_FOLDER="
set "NEXT_FOLDER="

REM Try .env file first
if exist ".env" (
    for /f "tokens=1,* delims==" %%A in ('findstr /b "VIDEO_FOLDER=" ".env"') do set "VIDEO_FOLDER=%%B"
    for /f "tokens=1,* delims==" %%A in ('findstr /b "NEXT_ROTATION_FOLDER=" ".env"') do set "NEXT_FOLDER=%%B"
)

REM Fall back to playlists.json if not found in .env
if "!VIDEO_FOLDER!"=="" (
    for /f "tokens=*" %%A in ('powershell -NoProfile -Command "try { $config = Get-Content 'config/playlists.json' | ConvertFrom-Json; Write-Output $config.settings.video_folder } catch { Write-Output '' }"') do set "VIDEO_FOLDER=%%A"
)
if "!NEXT_FOLDER!"=="" (
    for /f "tokens=*" %%A in ('powershell -NoProfile -Command "try { $config = Get-Content 'config/playlists.json' | ConvertFrom-Json; Write-Output $config.settings.next_rotation_folder } catch { Write-Output '' }"') do set "NEXT_FOLDER=%%A"
)

REM Final defaults
if "!VIDEO_FOLDER!"=="" set "VIDEO_FOLDER=videos/live"
if "!NEXT_FOLDER!"=="" set "NEXT_FOLDER=videos/pending"

REM Cleanup path formatting (remove trailing slashes)
if "!VIDEO_FOLDER:~-1!"=="\" set "VIDEO_FOLDER=!VIDEO_FOLDER:~0,-1!"
if "!VIDEO_FOLDER:~-1!"=="/" set "VIDEO_FOLDER=!VIDEO_FOLDER:~0,-1!"
if "!NEXT_FOLDER:~-1!"=="\" set "NEXT_FOLDER=!NEXT_FOLDER:~0,-1!"
if "!NEXT_FOLDER:~-1!"=="/" set "NEXT_FOLDER=!NEXT_FOLDER:~0,-1!"

echo.
echo Configured video folder: !VIDEO_FOLDER!
echo Configured next rotation folder: !NEXT_FOLDER!
echo.
echo WARNING: This will delete:
echo   - core\stream_data.db
echo   - All files in !VIDEO_FOLDER!
echo   - All files in !NEXT_FOLDER!
echo.
set /p CONFIRM="Type 'y' to confirm and continue: "
if /i not "!CONFIRM!"=="y" (
    echo Reset cancelled.
    pause
    exit /b 0
)

echo.

REM Delete database
if exist "core\stream_data.db" (
    echo Deleting stream_data.db...
    del /f /q "core\stream_data.db"
    if exist "core\stream_data.db" (
        echo WARNING: Failed to delete stream_data.db (may be in use)
    ) else (
        echo Successfully deleted stream_data.db
    )
) else (
    echo stream_data.db not found (already deleted)
)

echo.
REM Delete live videos
if exist "!VIDEO_FOLDER!" (
    echo Deleting !VIDEO_FOLDER!\*...
    del /f /q "!VIDEO_FOLDER!\*"
    for /d %%x in ("!VIDEO_FOLDER!\*") do @rmdir /s /q "%%x" 2>nul
    echo Cleared !VIDEO_FOLDER!
) else (
    echo !VIDEO_FOLDER! folder not found
)

echo.

REM Delete pending videos
if exist "!NEXT_FOLDER!" (
    echo Deleting !NEXT_FOLDER!\*...
    del /f /q "!NEXT_FOLDER!\*"
    for /d %%x in ("!NEXT_FOLDER!\*") do @rmdir /s /q "%%x" 2>nul
    echo Cleared !NEXT_FOLDER!
) else (
    echo !NEXT_FOLDER! folder not found
)

echo.

REM Delete backup folders (if they exist)
echo Cleaning backup folders...

REM Get parent directory of video folder for temp backup folders
for %%A in ("!VIDEO_FOLDER!") do set "VIDEO_PARENT=%%~dpA"
set "VIDEO_PARENT=!VIDEO_PARENT:~0,-1!"

REM Delete temp_pending_backup (prepared rotation backup)
if exist "!VIDEO_PARENT!\temp_pending_backup" (
    echo Deleting !VIDEO_PARENT!\temp_pending_backup\*...
    del /f /q "!VIDEO_PARENT!\temp_pending_backup\*" 2>nul
    for /d %%x in ("!VIDEO_PARENT!\temp_pending_backup\*") do @rmdir /s /q "%%x" 2>nul
    rmdir /q "!VIDEO_PARENT!\temp_pending_backup" 2>nul
)

echo.

REM Delete temp_playback folder (created during large playlist downloads)
if exist "!VIDEO_PARENT!\temp_playback" (
    echo Deleting !VIDEO_PARENT!\temp_playback\*...
    del /f /q "!VIDEO_PARENT!\temp_playback\*" 2>nul
    for /d %%x in ("!VIDEO_PARENT!\temp_playback\*") do @rmdir /s /q "%%x" 2>nul
    rmdir /q "!VIDEO_PARENT!\temp_playback" 2>nul
    echo Deleted temp_playback folder
) else (
    echo temp_playback folder not found
)

echo.
echo Reset complete!
echo You can now run main.py to start fresh.
pause
