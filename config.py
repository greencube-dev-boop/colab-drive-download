import os
import re
import uuid
import shutil
import threading
from pathlib import Path

# Detect Environment: Google Colab vs Local
IS_COLAB = os.path.exists("/content") and not os.path.exists("C:\\")

if IS_COLAB:
    DOWNLOADER_DIR = "/content/drive/MyDrive/downloader"
    LOCAL_WORKSPACE = "/content/temp"
else:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    DOWNLOADER_DIR = os.path.join(BASE_DIR, "downloader_staging")
    LOCAL_WORKSPACE = os.path.join(BASE_DIR, "content", "temp")

# Structured Google Drive Directories
RAW_DIR = os.path.join(DOWNLOADER_DIR, "raw")
REFINED_DIR = os.path.join(DOWNLOADER_DIR, "refined")
REFINED_MOVIES_DIR = os.path.join(REFINED_DIR, "movies")
REFINED_TV_DIR = os.path.join(REFINED_DIR, "tv_series")
REFINED_OTHER_DIR = os.path.join(REFINED_DIR, "other")
DATA_DIR = os.path.join(DOWNLOADER_DIR, "data")
METADATA_JSON = os.path.join(DATA_DIR, "metadata.json")
QUEUE_DB = os.path.join(DATA_DIR, "queue.db")

# Ephemeral Local VM Directories
LOCAL_RAW_DIR = os.path.join(LOCAL_WORKSPACE, "raw_in")
LOCAL_REFINE_DIR = os.path.join(LOCAL_WORKSPACE, "refining")
TRASH_STAGING_DIR = os.path.join(LOCAL_WORKSPACE, "trash_staging")
TEMP_DIR = LOCAL_WORKSPACE
DRIVE_DIR = DOWNLOADER_DIR

# Ensure necessary directories exist
for d in [
    DOWNLOADER_DIR, RAW_DIR, REFINED_DIR, REFINED_MOVIES_DIR,
    REFINED_TV_DIR, REFINED_OTHER_DIR, DATA_DIR,
    LOCAL_WORKSPACE, LOCAL_RAW_DIR, LOCAL_REFINE_DIR, TRASH_STAGING_DIR
]:
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass

# Concurrency Semaphores & Worker Limits
MAX_DOWNLOAD_WORKERS = 3
MAX_LOW_SPEED_DOWNLOAD_WORKERS = 2
MAX_REFINE_WORKERS = 1

download_semaphore = threading.Semaphore(MAX_DOWNLOAD_WORKERS)
low_speed_download_semaphore = threading.Semaphore(MAX_LOW_SPEED_DOWNLOAD_WORKERS)
conversion_semaphore = threading.Semaphore(MAX_REFINE_WORKERS)

# Timeout and Speed Watchdog Rules
DOWNLOAD_START_TIMEOUT_SEC = 90.0
DOWNLOAD_LOW_SPEED_THRESHOLD_BPS = 100 * 1024.0  # 100 KB/s
DOWNLOAD_LOW_SPEED_TIMEOUT_SEC = 120.0
MAX_DOWNLOAD_START_RETRIES = 2  # Initial + 2 retries = 3 total attempts

# In-memory Task Tracking for real-time live monitoring
tasks = []
tasks_lock = threading.Lock()
task_counter = 0

# Global worker controls
download_worker_active = True
refine_worker_active = True
refine_worker_lock = threading.Lock()

def sanitize_filename(name: str) -> str:
    """Sanitizes filename for cross-platform compatibility and Google Drive safety."""
    if not name:
        return "video"
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    name = re.sub(r'\s+', " ", name).strip()
    return name[:120] if len(name) > 120 else name

def bypass_trash_delete(file_path_on_drive: str) -> bool:
    """
    Bypasses Google Drive Trash Bin:
    Moves the file from Google Drive into the local VM ephemeral workspace first,
    then permanently removes it locally. This prevents Drive Trash quota consumption.
    """
    if not file_path_on_drive or not os.path.exists(file_path_on_drive):
        return False
    try:
        os.makedirs(TRASH_STAGING_DIR, exist_ok=True)
        unique_name = f"trash_{uuid.uuid4().hex[:8]}_{os.path.basename(file_path_on_drive)}"
        local_dest = os.path.join(TRASH_STAGING_DIR, unique_name)
        # Move off Google Drive onto local filesystem
        shutil.move(file_path_on_drive, local_dest)
        # Unlink from local disk
        if os.path.exists(local_dest):
            os.remove(local_dest)
        return True
    except Exception as e:
        print(f"[TrashBypass] Warning: Could not bypass trash for {file_path_on_drive}: {e}")
        try:
            if os.path.exists(file_path_on_drive):
                os.remove(file_path_on_drive)
                return True
        except Exception:
            pass
        return False
