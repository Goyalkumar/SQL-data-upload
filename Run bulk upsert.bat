@echo off
REM ============================================================================
REM Bulk UPSERT Script - Windows Launcher
REM ============================================================================

SETLOCAL EnableDelayedExpansion

REM Change to script directory
cd /d "%~dp0"

REM Check if Python is installed
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8 or higher
    pause
    exit /b 1
)

REM Check if .env file exists
if not exist ".env" (
    echo WARNING: .env file not found!
    echo Please copy .env.template to .env and configure your settings
    pause
    exit /b 1
)

REM Check if dependencies are installed
python -c "import pandas, pyodbc, dotenv" >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Installing required dependencies...
    pip install -r requirements.txt
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to install dependencies
        pause
        exit /b 1
    )
)

echo ============================================================================
echo Bulk UPSERT Script Launcher
echo ============================================================================
echo.
echo Options:
echo   1. Run normal execution (commit changes)
echo   2. Run dry-run mode (preview only)
echo   3. Run with custom CSV path
echo   4. Exit
echo.

set /p choice="Select option (1-4): "

if "%choice%"=="1" (
    echo.
    echo Starting normal execution...
    python bulk_upsert_enhanced.py
    set EXIT_CODE=!ERRORLEVEL!
) else if "%choice%"=="2" (
    echo.
    echo Starting dry-run mode...
    python bulk_upsert_enhanced.py --dry-run
    set EXIT_CODE=!ERRORLEVEL!
) else if "%choice%"=="3" (
    set /p csv_path="Enter CSV file path: "
    echo.
    echo Starting execution with custom path...
    python bulk_upsert_enhanced.py --csv-path "!csv_path!"
    set EXIT_CODE=!ERRORLEVEL!
) else if "%choice%"=="4" (
    echo Exiting...
    exit /b 0
) else (
    echo Invalid option
    pause
    exit /b 1
)

echo.
if !EXIT_CODE! EQU 0 (
    echo ============================================================================
    echo SUCCESS: Script completed successfully
    echo ============================================================================
) else (
    echo ============================================================================
    echo ERROR: Script failed with exit code !EXIT_CODE!
    echo Check the log files in the logs\ directory for details
    echo ============================================================================
)

echo.
echo Press any key to view the latest log file...
pause >nul

REM Show latest log file
for /f "delims=" %%i in ('dir /b /od logs\*.log') do set LATEST_LOG=%%i
if defined LATEST_LOG (
    type "logs\%LATEST_LOG%"
)

echo.
pause
exit /b !EXIT_CODE!
