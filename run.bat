@echo off
:: ============================================================
::  OpenStreamRotator — Run
::  Starts OpenStreamRotator from source.
:: ============================================================

echo.
echo  =======================================
echo   OpenStreamRotator - Starting
echo  =======================================
echo.

:: ----------------------------------------------------------
:: Pre-flight checks
:: ----------------------------------------------------------
if not exist ".env" (
    echo  ERROR: .env file not found.
    echo  Run setup.bat first to configure OSR.
    echo.
    pause
    exit /b 1
)

:: ----------------------------------------------------------
:: Activate venv if present
:: ----------------------------------------------------------
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
)

:: ----------------------------------------------------------
:: Check dependencies
:: ----------------------------------------------------------
python -c "import aiohttp" 2>nul
if %ERRORLEVEL% neq 0 (
    echo  Installing dependencies...
    pip install -r requirements.txt --quiet
    echo.
)

:: ----------------------------------------------------------
:: Start OSR
:: ----------------------------------------------------------
echo  Starting OpenStreamRotator...
echo  Press Ctrl+C to stop.
echo.
python main.py
echo.
pause
