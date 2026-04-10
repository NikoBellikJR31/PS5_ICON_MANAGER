#!/usr/bin/env python3
"""
PS5 Icon Manager - Local Server
by fsociety

Lance ce script et ouvre http://localhost:8001 dans ton navigateur
"""

import os
import sys
import webbrowser
import threading
import time

def open_browser():
    time.sleep(2)
    webbrowser.open('http://localhost:8001')

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════╗
    ║     PS5 ICON MANAGER - by fsociety        ║
    ║                                           ║
    ║  Serveur démarré sur http://localhost:8001║
    ║  Appuie sur Ctrl+C pour arrêter           ║
    ╚═══════════════════════════════════════════╝
    """)
    
    # Open browser automatically
    threading.Thread(target=open_browser, daemon=True).start()
    
    # Run uvicorn
    os.system("uvicorn server:app --host 0.0.0.0 --port 8001 --reload")
