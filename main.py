import subprocess
import sys
import time
import webbrowser
import threading
import os

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

os.chdir(BASE_DIR)
os.system("color 0A")
os.system("title PS5 Icon Manager - fsociety v3.0")

GREEN = "\033[92m"
RESET = "\033[0m"


def type_text(text, delay=0.003):
    for char in text:
        print(char, end="", flush=True)
        time.sleep(delay)
    print()


def loading(msg):
    print(msg, end="", flush=True)
    for _ in range(3):
        time.sleep(0.3)
        print(".", end="", flush=True)
    print("\n")


logo = r"""
╔════════════════════════════════════════════╗
║     PS5 ICON MANAGER • FSOCIETY  v3.0      ║
╚════════════════════════════════════════════╝
"""


print(GREEN)
type_text(logo, 0.001)
type_text(">> Boot sequence initialized", 0.01)
type_text(">> Checking environment...", 0.008)
time.sleep(0.3)
print("OK\n")


loading("[] Installation des dependances")
try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
    print("[OK] Dependances OK\n")
except Exception as e:
    print("[!] Erreur installation:", e)


def open_browser():
    time.sleep(1.5)
    webbrowser.open("http://127.0.0.1:8001")

threading.Thread(target=open_browser).start()

loading("[] Lancement du serveur")

print("[PC] http://127.0.0.1:8001\n")


try:
    subprocess.run([
        sys.executable, "-m", "uvicorn",
        "server:app",
        "--host", "127.0.0.1",
        "--port", "8001",
        "--log-level", "info"
    ])
except Exception as e:
    print("[!] Erreur serveur:", e)

print(RESET)
print("\n[!] Serveur arrete")
input("Appuyez sur Entree pour quitter...")
