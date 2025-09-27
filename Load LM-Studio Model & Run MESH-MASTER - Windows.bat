@echo off
cd /d "%USERPROFILE%\Desktop\mesh-master"  # Your MESH MASTER directory

echo Unloading any previously loaded model before reloading...
lms unload <INSERT MODEL IDENTIFIER HERE>
timeout /t 2 /nobreak >nul

echo Loading defined model...
lms load <INSERT MODEL IDENTIFIER HERE>
timeout /t 5 /nobreak >nul

echo Running MESH MASTER...
python mesh-master.py

pause
