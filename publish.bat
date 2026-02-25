@echo off
title Publish Contest Files
echo ========================================
echo   Publishing contest files to website
echo ========================================
echo.

cd /d "%~dp0"

:: Stage, commit, and push
git add *.json
git commit -m "Update contest files"
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
