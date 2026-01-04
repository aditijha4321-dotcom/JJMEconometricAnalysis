@echo off
echo ========================================================
echo  JJM ECONOMETRIC ANALYSIS - AUTO INSTALLER
echo ========================================================

:: 1. Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo CRITICAL ERROR: Python is not installed.
    echo Please install Python from python.org and try again.
    pause
    exit /b
)

:: 2. Create Virtual Environment (Hidden from user)
if not exist "venv_share" (
    echo Creating isolated environment...
    python -m venv venv_share
)

:: 3. Install Dependencies
echo Installing libraries (Pandas, Statsmodels, LinearModels)...
call venv_share\Scripts\activate
pip install -r requirements.txt >nul 2>&1

:: 4. Check for Data
if not exist "data\processed\final_panel_2019.csv" (
    echo.
    echo WARNING: Data file is missing!
    echo Please make sure 'final_panel_2019.csv' is in data\processed\
    pause
    exit /b
)

:: 5. Run the Analysis
echo.
echo Running State-Level Analysis...
python src/analyze_star_states.py

echo.
echo ========================================================
echo  SUCCESS! CHECK THE OUTPUT FOLDER FOR GRAPHS.
echo ========================================================
pause