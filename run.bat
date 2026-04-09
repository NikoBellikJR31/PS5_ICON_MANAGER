@echo off
title PS5 Icon Manager - fsociety v2.1
echo.
echo  ========================================
echo   PS5 ICON MANAGER - by fsociety v2.1
echo  ========================================
echo.
echo  Installation des dependances...
pip install -r requirements.txt
echo.
echo  Demarrage du serveur...
echo  Ouvre http://localhost:8001 dans ton navigateur
echo.
python -m uvicorn server:app --host 0.0.0.0 --port 8001 --reload --log-level info --access-log
pause
