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

:: Copy config files (only user-editable ones, not Python modules)
echo Copying config files...
if not exist dist\config mkdir dist\config
copy /Y config\playlists.json dist\config\playlists.json >nul
copy /Y config\settings.json dist\config\settings.json >nul

:: Copy .env.example as .env (NOT the real .env)
echo Copying .env.example as .env...
copy /Y .env.example dist\.env >nul

:: Create content directories
echo Creating content directories...
if not exist dist\content\live mkdir dist\content\live
if not exist dist\content\pending mkdir dist\content\pending
if not exist dist\content\pause mkdir dist\content\pause
if not exist dist\content\rotation mkdir dist\content\rotation
if not exist dist\content\prepared mkdir dist\content\prepared

:: Copy default screen images
echo Copying default screen images...
copy /Y content\pause\default.png dist\content\pause\default.png >nul
copy /Y content\rotation\default.png dist\content\rotation\default.png >nul

:: Copy setup scripts
echo Copying setup scripts...
copy /Y setup.bat dist\setup.bat >nul
copy /Y setup.sh dist\setup.sh >nul

echo.
echo === Build complete! ===
echo Output: dist\OpenStreamRotator.exe
echo.
echo Remember: Run setup.bat to configure, or edit dist\.env manually.
pause
