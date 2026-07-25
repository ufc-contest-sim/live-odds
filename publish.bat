@echo off
title Publish Contest Files
echo ========================================
echo   Publishing contest files to website
echo ========================================
echo.

cd /d "%~dp0"

:: Stage, commit, and push. The stats CSV is only staged when it exists
:: locally, so a missing local copy never deletes it from the site.
git add *.json
:: Salary file staged only when present under its exact name, so a
:: missing/misnamed DKSalaries.csv cannot abort staging everything else.
if exist DKSalaries.csv git add DKSalaries.csv
if not exist DKSalaries.csv echo NOTE: DKSalaries.csv not found - check the filename has no spaces.
if exist ufc_fight_stats.csv git add ufc_fight_stats.csv
:: Site icons (favicon), when present
if exist favicon.ico git add favicon.ico
if exist favicon-96x96.png git add favicon-96x96.png
if exist favicon-192x192.png git add favicon-192x192.png
if exist favicon-512x512.png git add favicon-512x512.png
if exist apple-touch-icon.png git add apple-touch-icon.png
git commit -m "Update contest and salary files"
if %errorlevel% neq 0 (
    echo.
    echo Nothing new to publish — files are already up to date.
    echo.
    pause
    exit /b
)

echo.
echo Syncing with GitHub...
git pull --no-rebase -X ours origin main
if %errorlevel% neq 0 (
    echo.
    echo Pull failed. Check your internet connection or resolve conflicts.
    echo.
    pause
    exit /b
)

echo.
echo Pushing to GitHub...
git push
if %errorlevel% neq 0 (
    echo.
    echo Push failed. Check your internet connection.
    echo.
    pause
    exit /b
)

echo.
echo ========================================
echo   Done! Site will update in a minute.
echo ========================================
echo.
pause
