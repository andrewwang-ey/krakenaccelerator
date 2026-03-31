@echo off
cd /d "%~dp0"

echo ============================================
echo   Kraken Migration Accelerator — First Time Setup
echo ============================================
echo.

:: Check Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed on this machine.
    echo.
    echo Please download and install Python from:
    echo   https://www.python.org/downloads/
    echo.
    echo Make sure to tick "Add Python to PATH" during installation.
    echo Then run this setup again.
    pause
    exit /b 1
)

echo Python found. Creating virtual environment...
python -m venv venv

echo.
echo Installing dependencies ^(this may take a minute^)...
call venv\Scripts\activate.bat
pip install --quiet dbt-duckdb duckdb pyyaml

echo.
echo ============================================
echo   Setup complete!
echo   You can now run: run_pipeline.bat
echo ============================================
echo.
pause