@echo off
REM Absolute Th Path
cd C:\Users\User_Name\dj_risk_compliance

REM Activate virtual environment 
call venv\Scripts\activate.bat

REM Run the My cron script
python -m cron.dowjones_cron

