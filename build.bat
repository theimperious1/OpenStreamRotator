@echo off
echo === Building OpenStreamRotator ===
echo.

:: Build the exe
pyinstaller OpenStreamRotator.spec --noconfirm
if %ERRORLEVEL% neq 0 (
    echo Build failed!
    pause
    exit /b 1
)

set OUT=dist\OpenStreamRotator

:: Copy config files (only user-editable ones, not Python modules)
echo Copying config files...
if not exist %OUT%\config mkdir %OUT%\config
copy /Y config\playlists.json %OUT%\config\playlists.json >nul
copy /Y config\settings.json %OUT%\config\settings.json >nul

:: Copy .env.example as .env (NOT the real .env)
echo Copying .env.example as .env...
copy /Y .env.example %OUT%\.env >nul

:: Create content directories
echo Creating content directories...
if not exist %OUT%\content\live mkdir %OUT%\content\live
if not exist %OUT%\content\pending mkdir %OUT%\content\pending
if not exist %OUT%\content\pause mkdir %OUT%\content\pause
if not exist %OUT%\content\rotation mkdir %OUT%\content\rotation
if not exist %OUT%\content\prepared mkdir %OUT%\content\prepared

:: Copy default screen images
echo Copying default screen images...
copy /Y content\pause\default.png %OUT%\content\pause\default.png >nul
copy /Y content\rotation\default.png %OUT%\content\rotation\default.png >nul

:: Copy setup scripts
echo Copying setup scripts...
copy /Y setup.bat %OUT%\setup.bat >nul
copy /Y setup.sh %OUT%\setup.sh >nul

echo.
echo === Build complete! ===
echo Output: %OUT%\OpenStreamRotator.exe
echo.
echo Remember: Run setup.bat to configure, or edit %OUT%\.env manually.
pause
