@echo off
title Publish Contest Files
echo ========================================
echo   Publishing contest files to website
echo ========================================
echo.

cd /d "%~dp0"

:: Browsers save repeat downloads as "DKSalaries (1).csv" instead of
:: replacing DKSalaries.csv. If a stray copy like that is NEWER than the
:: current DKSalaries.csv, adopt it automatically so dropping the fresh
:: download anywhere in this folder is enough.
powershell -NoProfile -Command "$c = Get-Item 'DKSalaries.csv' -ErrorAction SilentlyContinue; $s = Get-ChildItem 'DKSalaries*.csv' -ErrorAction SilentlyContinue | Where-Object { $_.Name -ne 'DKSalaries.csv' } | Sort-Object LastWriteTime -Descending | Select-Object -First 1; if ($s -and (-not $c -or $s.LastWriteTime -gt $c.LastWriteTime)) { Move-Item -LiteralPath $s.FullName -Destination 'DKSalaries.csv' -Force; Write-Host ('Found newer download ' + $s.Name + ' - using it as DKSalaries.csv') }" 2>nul

:: Stage only the files this script manages. A file that is missing
:: locally is simply not staged, so it can never be deleted from the site.
git add *.json >nul 2>&1
if exist DKSalaries.csv git add DKSalaries.csv
if not exist DKSalaries.csv echo NOTE: DKSalaries.csv not found - check the filename has no spaces.
if exist ufc_fight_stats.csv git add ufc_fight_stats.csv
:: Site icons (favicon), when present
if exist favicon.ico git add favicon.ico
if exist favicon-96x96.png git add favicon-96x96.png
if exist favicon-192x192.png git add favicon-192x192.png
if exist favicon-512x512.png git add favicon-512x512.png
if exist apple-touch-icon.png git add apple-touch-icon.png

:: Did any of the managed files actually change?
git diff --cached --quiet
if %errorlevel% equ 0 goto nothingnew

echo Publishing these files:
git --no-pager diff --cached --name-only
echo.
git commit -m "Update contest and salary files" >nul

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
exit /b

:nothingnew
echo.
echo Nothing new to publish - the contest JSONs, DKSalaries.csv and
echo ufc_fight_stats.csv in THIS folder are identical to what is
echo already on the website.
echo.
echo If you just updated those files, the new versions are probably not
echo in this folder (or were saved under a different name). The copies
echo here were last modified:
for %%F in (DKSalaries.csv ufc_fight_stats.csv) do if exist "%%F" echo     %%F  -  %%~tF
:: Repeated browser downloads save as "DKSalaries (1).csv" etc. - warn,
:: because the site only reads the exact name DKSalaries.csv.
dir /b "DKSalaries*.csv" 2>nul | findstr /v /i /x "DKSalaries.csv" >nul 2>&1
if %errorlevel% equ 0 (
    echo.
    echo WARNING: extra salary-file copies found in this folder. The site
    echo only reads the exact name DKSalaries.csv - if one of these is
    echo your new download, rename it to DKSalaries.csv and re-run:
    dir /b "DKSalaries*.csv" | findstr /v /i /x "DKSalaries.csv"
)
echo.
pause
exit /b
