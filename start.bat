@echo off
setlocal
cd /d "%~dp0"
where py >nul 2>nul
if not errorlevel 1 (
    py -3 bootstrap.py %*
) else (
    where python >nul 2>nul
    if errorlevel 1 (
        echo Python 3.11+ is required. Please install Python and add it to PATH.
        pause
        exit /b 2
    )
    python bootstrap.py %*
)
if errorlevel 1 (
    echo.
    echo QA Sentinel did not start. Check the error above.
    pause
    exit /b 2
)
