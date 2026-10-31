import os
import re
import uuid
import time
import shutil
import threading
from typing import Optional, List, Dict, Any
from pathlib import Path

# Detect Environment: Google Colab vs Local
IS_COLAB = os.path.exists("/content") and not os.path.exists("C:\\")

def resolve_downloader_dir() -> str:
    """
    Dynamically locates Google Drive downloader directory, supporting shortcuts,
    .shortcut-targets-by-id resolution, and shared folders while guaranteeing files
    are NEVER written directly to Google Drive root.
    """
    if not IS_COLAB:
        base_dir = os.path.abspath(os.path.dirname(__file__))
        return os.path.join(base_dir, "downloader_staging")

    # 1. Primary check: Current working directory (if mounted under Google Drive)
    cwd = os.path.abspath(os.getcwd())
    if cwd.startswith("/content/drive/") and cwd not in [os.path.abspath("/content/drive"), os.path.abspath("/content/drive/MyDrive"), os.path.abspath("/content/drive/Shareddrives")]:
        try:
            entries = [e.lower() for e in os.listdir(cwd)]
            if any(k in entries for k in ["data", "raw", "refined"]) or any(e.endswith(".ipynb") for e in entries):
                return cwd
            if os.path.exists(os.path.join(cwd, "data", "cluster", "master.json")):
                return cwd
        except Exception:
            pass

    # 2. Check .shortcut-targets-by-id (Google Drive shortcut targets on Colab)
    targets_root = "/content/drive/.shortcut-targets-by-id"
    if os.path.exists(targets_root):
        try:
            for tid in os.listdir(targets_root):
                t_path = os.path.join(targets_root, tid)
                if not os.path.isdir(t_path):
                    continue
                try:
                    entries = [e.lower() for e in os.listdir(t_path)]
                    if os.path.exists(os.path.join(t_path, "data", "cluster", "master.json")):
                        return t_path
                    if any(k in entries for k in ["data", "raw", "refined"]) or any(e.endswith(".ipynb") for e in entries):
                        return t_path
                    sub = os.path.join(t_path, "downloader")
                    if os.path.exists(sub) and os.path.isdir(sub):
                        return sub
                except Exception:
                    continue
        except Exception:
            pass

    # 3. Check canonical candidate directories
    candidates = [
        "/content/drive/MyDrive/colab drive download",
        "/content/drive/MyDrive/colab-drive-download",
        "/content/drive/MyDrive/colab_drive_download",
        "/content/drive/MyDrive/downloader",
        "/content/drive/MyDrive/downloader - Shortcut",
        "/content/drive/Shareddrives/colab drive download",
        "/content/drive/Shareddrives/colab-drive-download",
        "/content/drive/Shareddrives/downloader"
    ]
    for c in candidates:
        if os.path.exists(c):
            try:
                real = os.path.realpath(c)
                if real and os.path.exists(real) and os.path.isdir(real):
                    if os.path.abspath(real) not in [os.path.abspath("/content/drive/MyDrive"), os.path.abspath("/content/drive")]:
                        return real
            except Exception:
                pass
            if os.path.isdir(c):
                return c

    # 4. Search /content/drive/MyDrive for any folder containing data/cluster or matching name
    mydrive = "/content/drive/MyDrive"
    if os.path.exists(mydrive):
        try:
            entries = os.listdir(mydrive) # Busts FUSE directory cache
            for entry in entries:
                full_p = os.path.join(mydrive, entry)
                if not os.path.isdir(full_p):
                    continue
                try:
                    if os.path.exists(os.path.join(full_p, "data", "cluster", "master.json")) or os.path.exists(os.path.join(full_p, "data", "queue.db")):
                        return full_p
                except Exception:
                    pass
                name_l = entry.lower()
                if name_l.startswith("downloader") or ("colab" in name_l and "download" in name_l):
                    try:
                        real = os.path.realpath(full_p)
                        if real and os.path.exists(real) and os.path.isdir(real):
                            if os.path.abspath(real) not in [os.path.abspath("/content/drive/MyDrive"), os.path.abspath("/content/drive")]:
                                return real
                    except Exception:
                        pass
                    return full_p
        except Exception:
            pass

    # 5. Search Shared Drives
    shareddrives = "/content/drive/Shareddrives"
    if os.path.exists(shareddrives):
        try:
            entries = os.listdir(shareddrives)
            for entry in entries:
                full_p = os.path.join(shareddrives, entry)
                if not os.path.isdir(full_p):
                    continue
                try:
                    if os.path.exists(os.path.join(full_p, "data", "cluster", "master.json")):
                        return full_p
                except Exception:
                    pass
                name_l = entry.lower()
                if name_l.startswith("downloader") or ("colab" in name_l and "download" in name_l):
                    return full_p
                sub_p = os.path.join(full_p, "downloader")
                if os.path.exists(sub_p) and os.path.isdir(sub_p):
                    return sub_p
        except Exception:
            pass

    # Default fallback canonical path
    resolved = "/content/drive/MyDrive/colab drive download"
    if not os.path.exists(resolved):
        resolved = "/content/drive/MyDrive/downloader"
    resolved = os.path.abspath(resolved)
    if resolved in [os.path.abspath("/content/drive/MyDrive"), os.path.abspath("/content/drive"), os.path.abspath("/")]:
        resolved = "/content/drive/MyDrive/downloader"

    return resolved

DOWNLOADER_DIR = resolve_downloader_dir()
if IS_COLAB:
    LOCAL_WORKSPACE = "/content/temp"
else:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    LOCAL_WORKSPACE = os.path.join(BASE_DIR, "content", "temp")

# Structured Google Drive Directories
RAW_DIR = os.path.join(DOWNLOADER_DIR, "raw")
REFINED_DIR = os.path.join(DOWNLOADER_DIR, "refined")
REFINED_MOVIES_DIR = os.path.join(REFINED_DIR, "movies")
REFINED_TV_DIR = os.path.join(REFINED_DIR, "tv_series")
REFINED_OTHER_DIR = os.path.join(REFINED_DIR, "other")
DATA_DIR = os.path.join(DOWNLOADER_DIR, "data")
METADATA_JSON = os.path.join(DATA_DIR, "metadata.json")

# Ephemeral Local VM Directories
LOCAL_RAW_DIR = os.path.join(LOCAL_WORKSPACE, "raw_in")
LOCAL_REFINE_DIR = os.path.join(LOCAL_WORKSPACE, "refining")
TRASH_STAGING_DIR = os.path.join(LOCAL_WORKSPACE, "trash_staging")
LOCAL_DATA_DIR = os.path.join(LOCAL_WORKSPACE, "data")
LOCAL_METADATA_JSON = os.path.join(LOCAL_DATA_DIR, "metadata.json")
TEMP_DIR = LOCAL_WORKSPACE
DRIVE_DIR = DOWNLOADER_DIR

# Distributed GPU Cluster & Chunks Staging Directories
STAGING_DIR = os.path.join(DOWNLOADER_DIR, "staging")
CHUNKS_STAGING_DIR = os.path.join(STAGING_DIR, "chunks")
LOCAL_CHUNKS_DIR = os.path.join(LOCAL_WORKSPACE, "chunks")
CLUSTER_DIR = os.path.join(DATA_DIR, "cluster")
CLUSTER_WORKERS_DIR = os.path.join(CLUSTER_DIR, "workers")
CLUSTER_MASTER_FILE = os.path.join(CLUSTER_DIR, "master.json")
LOCAL_CLUSTER_DIR = os.path.join(LOCAL_DATA_DIR, "cluster")
LOCAL_CLUSTER_WORKERS_DIR = os.path.join(LOCAL_CLUSTER_DIR, "workers")
LOCAL_CLUSTER_MASTER_FILE = os.path.join(LOCAL_CLUSTER_DIR, "master.json")

# Distributed Cluster Configuration
CHUNK_DURATION_SEC = 240.0         # 4 minutes per keyframe segment (optimal balance of fault tolerance & low Drive FUSE I/O)
CHUNK_LEASE_TIMEOUT_SEC = 300.0    # 5 minutes lease duration (prevents false abandonments during 4-min chunk transcodes)
WORKER_HEARTBEAT_TIMEOUT_SEC = 15.0 # Mark worker offline and delete if no heartbeat for 15s
MAX_ACTIVE_CLUSTER_JOBS = 2        # Continuous pipeline: pre-buffers next job's chunks so GPU workers never sit idle

# Verbose Logging & Debug Notifications Toggle
DEBUG_LOGS: bool = (os.environ.get("ENABLE_DEBUG_LOGS", "1") != "0")
SYSTEM_LOG_FILE: str = os.path.join(DATA_DIR, "system.log")

# Database Paths: Google Drive vs Local working copy
DRIVE_QUEUE_DB = os.path.join(DATA_DIR, "queue.db")
LOCAL_QUEUE_DB = os.path.join(LOCAL_DATA_DIR, "queue.db")

# In Colab, always use local SSD SQLite file to eliminate Google Drive FUSE file-locking failures; otherwise use DRIVE_QUEUE_DB
if IS_COLAB:
    QUEUE_DB = LOCAL_QUEUE_DB
else:
    QUEUE_DB = DRIVE_QUEUE_DB

def update_downloader_dir(new_dir: str) -> None:
    """Dynamically points all Drive storage paths to a newly discovered downloader directory."""
    global DOWNLOADER_DIR, RAW_DIR, REFINED_DIR, REFINED_MOVIES_DIR, REFINED_TV_DIR, REFINED_OTHER_DIR
    global DATA_DIR, METADATA_JSON, DRIVE_DIR, STAGING_DIR, CHUNKS_STAGING_DIR, CLUSTER_DIR
    global CLUSTER_WORKERS_DIR, CLUSTER_MASTER_FILE, SYSTEM_LOG_FILE, DRIVE_QUEUE_DB, QUEUE_DB

    if not new_dir or not os.path.isdir(new_dir):
        return

    DOWNLOADER_DIR = os.path.abspath(new_dir)
    RAW_DIR = os.path.join(DOWNLOADER_DIR, "raw")
    REFINED_DIR = os.path.join(DOWNLOADER_DIR, "refined")
    REFINED_MOVIES_DIR = os.path.join(REFINED_DIR, "movies")
    REFINED_TV_DIR = os.path.join(REFINED_DIR, "tv_series")
    REFINED_OTHER_DIR = os.path.join(REFINED_DIR, "other")
    DATA_DIR = os.path.join(DOWNLOADER_DIR, "data")
    METADATA_JSON = os.path.join(DATA_DIR, "metadata.json")
    DRIVE_DIR = DOWNLOADER_DIR
    STAGING_DIR = os.path.join(DOWNLOADER_DIR, "staging")
    CHUNKS_STAGING_DIR = os.path.join(STAGING_DIR, "chunks")
    CLUSTER_DIR = os.path.join(DATA_DIR, "cluster")
    CLUSTER_WORKERS_DIR = os.path.join(CLUSTER_DIR, "workers")
    CLUSTER_MASTER_FILE = os.path.join(CLUSTER_DIR, "master.json")
    SYSTEM_LOG_FILE = os.path.join(DATA_DIR, "system.log")
    DRIVE_QUEUE_DB = os.path.join(DATA_DIR, "queue.db")
    if not IS_COLAB:
        QUEUE_DB = DRIVE_QUEUE_DB

    for d in [
        DOWNLOADER_DIR, RAW_DIR, REFINED_DIR, REFINED_MOVIES_DIR,
        REFINED_TV_DIR, REFINED_OTHER_DIR, DATA_DIR, STAGING_DIR, CHUNKS_STAGING_DIR, CLUSTER_DIR, CLUSTER_WORKERS_DIR
    ]:
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            pass

# Ensure necessary directories exist
for d in [
    DOWNLOADER_DIR, RAW_DIR, REFINED_DIR, REFINED_MOVIES_DIR,
    REFINED_TV_DIR, REFINED_OTHER_DIR, DATA_DIR, STAGING_DIR, CHUNKS_STAGING_DIR, CLUSTER_DIR, CLUSTER_WORKERS_DIR,
    LOCAL_WORKSPACE, LOCAL_RAW_DIR, LOCAL_REFINE_DIR, TRASH_STAGING_DIR, LOCAL_DATA_DIR, LOCAL_CHUNKS_DIR, LOCAL_CLUSTER_DIR, LOCAL_CLUSTER_WORKERS_DIR
]:
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass

# Concurrency Semaphores & Worker Limits
MAX_DOWNLOAD_WORKERS = 3
MAX_LOW_SPEED_DOWNLOAD_WORKERS = 0  # 0 indicates UNLIMITED concurrent low-speed downloads
MAX_REFINE_WORKERS = MAX_ACTIVE_CLUSTER_JOBS

download_semaphore = threading.Semaphore(MAX_DOWNLOAD_WORKERS)
low_speed_download_semaphore = None  # None: unlimited concurrency for slow-lane trickle downloads
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

def cleanup_drive_root_stray_files(drive_root: Optional[str] = None):
    """
    Scans Google Drive root (/content/drive/MyDrive) for stray .db or .json files
    accidentally written there by shortcut resolution issues or FUSE ghost copies,
    and safely removes them using bypass_trash_delete.
    """
    if drive_root is None and not IS_COLAB:
        return
    mydrive = drive_root or "/content/drive/MyDrive"
    if not os.path.exists(mydrive):
        return

    try:
        entries = os.listdir(mydrive)
    except Exception:
        return

    # Patterns for stray database and metadata files
    stray_patterns = [
        re.compile(r"^queue.*\.db.*$", re.IGNORECASE),
        re.compile(r"^metadata.*\.json.*$", re.IGNORECASE),
    ]

    # Data preservation before cleanup:
    # If DATA_DIR does not have queue.db or metadata.json, rescue the best file from root
    try:
        dest_db = os.path.join(DATA_DIR, "queue.db")
        if not os.path.exists(dest_db):
            root_db = os.path.join(mydrive, "queue.db")
            if os.path.exists(root_db) and os.path.getsize(root_db) > 0:
                os.makedirs(DATA_DIR, exist_ok=True)
                shutil.copyfile(root_db, dest_db)
                print(f"[Cleanup] Rescued valid queue.db from Drive root to {dest_db}")

        dest_meta = os.path.join(DATA_DIR, "metadata.json")
        if not os.path.exists(dest_meta):
            root_meta = os.path.join(mydrive, "metadata.json")
            if os.path.exists(root_meta) and os.path.getsize(root_meta) > 0:
                os.makedirs(DATA_DIR, exist_ok=True)
                shutil.copyfile(root_meta, dest_meta)
                print(f"[Cleanup] Rescued valid metadata.json from Drive root to {dest_meta}")
    except Exception as e:
        print(f"[Cleanup] Warning checking for rescue files: {e}")

    cleaned = 0
    for entry in entries:
        full_path = os.path.join(mydrive, entry)
        if not os.path.isfile(full_path):
            continue
        for pat in stray_patterns:
            if pat.match(entry):
                print(f"[Cleanup] Removing stray root artifact: {entry}")
                bypass_trash_delete(full_path)
                cleaned += 1
                break
    if cleaned > 0:
        print(f"[Cleanup] Cleaned up {cleaned} stray db/json files from Google Drive root.")

# Run root cleanup on module import if running in Colab
if IS_COLAB:
    cleanup_drive_root_stray_files()
