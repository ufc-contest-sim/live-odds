@echo off
title Download DK Standings
cd /d "%~dp0"

:: Auto-elevate to admin so browser_cookie3 can read your browser's
:: cookie file even while the browser is open (that was the RequiresAdminError).
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo Requesting administrator access so it can read your logged-in browser...
    powershell -NoProfile -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)
cd /d "%~dp0"

echo ========================================
echo   Downloading contest standings CSVs
echo ========================================
echo.

:: Pulls standings for every contest in contests_to_track.txt using your
:: logged-in DraftKings session. Run this AFTER the slate locks.
python download_standings.py
if %errorlevel% equ 9009 (
    echo python not found, trying py ...
    py download_standings.py
)

echo.
pause
