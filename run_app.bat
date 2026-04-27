@echo off
cd /d "%~dp0"

if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Setup has not been run yet.
    echo.
    echo Please double-click run_setup.bat first.
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
python app.py
call venv\Scripts\deactivate.bat
