import sys
import os
import time
import threading
import urllib.request

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

os.chdir(BASE_DIR)

LOG_FILE = os.path.join(BASE_DIR, "debug.log")

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{time.strftime('%H:%M:%S')} - {msg}\n")
    print(msg)

log("=== DEMARRAGE ===")
log(f"BASE_DIR: {BASE_DIR}")
log(f"sys.frozen: {getattr(sys, 'frozen', False)}")
log(f"Fichiers presents: {os.listdir(BASE_DIR)}")

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

def start_server():
    try:
        log("Thread serveur demarre")
        import importlib.util
        server_path = os.path.join(BASE_DIR, "server.py")
        log(f"server.py existe: {os.path.exists(server_path)}")
        spec = importlib.util.spec_from_file_location("server", server_path)
        server_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(server_module)
        log("server.py charge OK")
        import uvicorn
        log("uvicorn OK, lancement...")
        uvicorn.run(
    server_module.app,
    host="127.0.0.1",
    port=8001,
    log_config=None,   
)
    except Exception as e:
        import traceback
        log(f"ERREUR: {e}")
        log(traceback.format_exc())

def wait_for_server(timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            urllib.request.urlopen("http://127.0.0.1:8001", timeout=1)
            log("Serveur repond OK!")
            return True
        except Exception as e:
            log(f"Attente... {e}")
            time.sleep(1)
    return False

if __name__ == "__main__":
    t = threading.Thread(target=start_server, daemon=True)
    t.start()

    ok = wait_for_server(30)

    if not ok:
        log("TIMEOUT - serveur n'a pas repondu")
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Erreur", f"Serveur non demarre.\nVoir debug.log dans:\n{BASE_DIR}")
        sys.exit(1)

    import webview
    window = webview.create_window(
        "PS5 Icon Manager - fsociety v3.0",
        "http://127.0.0.1:8001",
        width=1280,
        height=800,
        min_size=(900, 600),
        background_color="#0a0a0a"
    )
    webview.start()