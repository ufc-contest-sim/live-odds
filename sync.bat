@echo off
title Sync Website Files
echo ========================================
echo   Syncing with GitHub
echo   (discard local index.html, pull latest)
echo ========================================
echo.

cd /d "%~dp0"

:: index.html is managed on GitHub (pushed by the dev side), so any local
:: copy is stale. Discard it so the pull can fast-forward cleanly.
echo Discarding local changes to index.html...
git restore index.html

echo.
echo Pulling latest from GitHub...
git pull origin main
if %errorlevel% neq 0 (
    echo.
    echo Pull failed. Check your internet connection or resolve conflicts.
    echo.
    pause
    exit /b
)

echo.
echo ========================================
echo   Done! You are up to date.
echo ========================================
echo.
pause
