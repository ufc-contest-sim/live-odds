@echo off
title Download DK Standings
echo ========================================
echo   Downloading contest standings CSVs
echo ========================================
echo.

cd /d "%~dp0"

:: Pulls standings for every contest in contests_to_track.txt using your
:: logged-in DraftKings session. Run this AFTER the slate locks.
python download_standings.py
if %errorlevel% neq 0 (
    echo.
    echo If you saw "'python' is not recognized", change  python  to  py  above.
    echo.
)

echo.
pause
