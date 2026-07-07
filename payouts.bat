@echo off
title Fetch DraftKings Payouts
echo ========================================
echo   Fetching this week's DK contest payouts
echo ========================================
echo.

cd /d "%~dp0"

:: Pulls the MMA lobby + every contest's full payout table into dk_payouts\.
:: Run this each slate before you run the sim.
python scrape_dk_contests.py --payouts
if %errorlevel% neq 0 (
    echo.
    echo Something went wrong. If you saw "'python' is not recognized",
    echo open this file and change  python  to  py  on the line above.
    echo.
    pause
    exit /b
)

echo.
echo ========================================
echo   Done. Payout tables saved to dk_payouts\
echo ========================================
echo.
pause
