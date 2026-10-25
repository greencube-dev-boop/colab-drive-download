import os
import re
import threading
import shutil

# Detect Environment: Google Colab vs Local
IS_COLAB = os.path.exists("/content") and not os.path.exists("C:\\")

if IS_COLAB:
    RAW_DIR = "/content/raw"
    TEMP_DIR = "/content/temp"
    DRIVE_DIR = "/content/drive/MyDrive/vid"
else:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    RAW_DIR = os.path.join(BASE_DIR, "content", "raw")
    TEMP_DIR = os.path.join(BASE_DIR, "content", "temp")
    DRIVE_DIR = os.path.join(BASE_DIR, "content", "drive", "MyDrive", "vid")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(DRIVE_DIR, exist_ok=True)

# Concurrency Semaphores (3 Simultaneous Downloads, 1 Dedicated GPU Transcoder)
download_semaphore = threading.Semaphore(3)
conversion_semaphore = threading.Semaphore(1)

# Task Queue & State Management
tasks = []
tasks_lock = threading.Lock()
task_counter = 0

def sanitize_filename(name: str) -> str:
    """Sanitizes filename for cross-platform compatibility and Google Drive safety."""
    if not name:
        return "video"
    name = re.sub(r'[\\\\/*?:"<>|]', "", name)
    name = re.sub(r'\\s+', " ", name).strip()
    return name[:120] if len(name) > 120 else name
