import os
import glob
import shutil
import threading
from typing import Dict, Any, List, Tuple
from config import (
    RAW_DIR, TEMP_DIR, download_semaphore, conversion_semaphore,
    tasks, tasks_lock, sanitize_filename
)
import config
from resolvers import resolve_video_source
from downloader import (
    download_hls_stream, download_ytdlp, download_aria2c_or_requests, download_aria2c_torrent
)
from encoder import convert_worker, move_to_drive_worker

def cleanup_task_files(task_id: int, task: Dict[str, Any] = None):
    """Deletes all local raw, temp, chunks, and intermediate files for a given task."""
    shutil.rmtree(os.path.join(TEMP_DIR, f"chunks_{task_id}"), ignore_errors=True)

    patterns = [
        os.path.join(TEMP_DIR, f"audio_{task_id}.opus"),
        os.path.join(TEMP_DIR, f"stitched_{task_id}.mkv"),
        os.path.join(TEMP_DIR, f"concat_{task_id}.txt"),
        os.path.join(TEMP_DIR, f"*_{task_id}*"),
        os.path.join(RAW_DIR, f"task_{task_id}_*")
    ]
    for pat in patterns:
        for p in glob.glob(pat):
            try:
                if os.path.isfile(p):
                    os.remove(p)
                elif os.path.isdir(p):
                    shutil.rmtree(p, ignore_errors=True)
            except Exception:
                pass

    if task:
        for key in ["raw_path", "temp_path", "url"]:
            p = task.get(key)
            if p and os.path.exists(p) and (key != "url" or task.get("is_torrent_file")):
                try:
                    os.remove(p)
                except Exception:
                    pass

def cancel_task_by_id(task_id: int) -> bool:
    """Cancels an active or queued process, kills its subprocess, and purges all files."""
    found = False
    with tasks_lock:
        for t in tasks:
            if str(t.get("id")) == str(task_id):
                t["cancelled"] = True
                t["status"] = "Cancelled"
                proc = t.get("active_proc")
                if proc:
                    try:
                        proc.terminate()
                        proc.kill()
                    except Exception:
                        pass
                t["active_proc"] = None
                cleanup_task_files(task_id, t)
                found = True
                break
    return found

def clear_done_tasks():
    """Removes completed, errored, and cancelled tasks from the process panel."""
    with tasks_lock:
        config.tasks = [t for t in config.tasks if t.get("status") not in ["Done", "Error", "Cancelled"]]

def download_worker(task: Dict[str, Any]):
    os.makedirs(RAW_DIR, exist_ok=True)

    # 1. Resolve source
    resolved = resolve_video_source(task["url"], task=task)
    source_url = resolved["url"]
    source_type = resolved["type"]
    headers = resolved["headers"]

    # 2. Set title & file extension (preserve user's custom name if set)
    if not task.get("has_custom_title") and resolved.get("title"):
        task["title"] = sanitize_filename(resolved["title"])

    ext = ".ts" if source_type == "videm_hls" else ".mp4"
    target_name = f"task_{task['id']}_{sanitize_filename(task['title'])}{ext}"
    task["raw_path"] = os.path.join(RAW_DIR, target_name)

    if task.get("cancelled"):
        return

    # 3. Route to optimal downloader
    if source_type == "local_torrent":
        largest_fp = download_aria2c_torrent(source_url, task)
        if largest_fp and os.path.exists(largest_fp):
            ext = os.path.splitext(largest_fp)[1].lower() or ".mkv"
            dest_path = os.path.join(RAW_DIR, f"task_{task['id']}_{sanitize_filename(task['title'])}{ext}")
            shutil.move(largest_fp, dest_path)
            shutil.rmtree(os.path.dirname(largest_fp), ignore_errors=True)
            task["raw_path"] = dest_path
    elif source_type == "videm_hls":
        download_hls_stream(source_url, headers, task["raw_path"], task, videm_meta=resolved.get("videm_meta"))
    elif source_type == "m3u8":
        try:
            download_hls_stream(source_url, headers, task["raw_path"], task)
        except Exception:
            download_ytdlp(source_url, task["raw_path"], headers, task)
    elif source_type == "ytdlp":
        download_ytdlp(source_url, task["raw_path"], headers, task)
    else:
        download_aria2c_or_requests(source_url, task["raw_path"], task)

def run_task_pipeline(task: Dict[str, Any]):
    t_id = task.get("id")
    try:
        if task.get("cancelled"):
            return

        task["status"] = "Queued for Download"
        with download_semaphore:
            if task.get("cancelled"):
                return
            task["status"] = "Downloading"
            download_worker(task)

        if task.get("cancelled"):
            return

        task["status"] = "Queued for Conversion"
        with conversion_semaphore:
            if task.get("cancelled"):
                return
            task["status"] = "Converting"
            convert_worker(task)

        if task.get("cancelled"):
            return

        task["status"] = "Moving to Drive"
        move_to_drive_worker(task)

        task["status"] = "Done"

    except Exception as e:
        task["status"] = "Error"
        task["error_msg"] = str(e)
    finally:
        if task.get("cancelled"):
            task["status"] = "Cancelled"
            cleanup_task_files(t_id, task)

def queue_direct_items(items_list: List[Tuple[str, str]], workers_val: int = 3, debrid_prov: str = "local", debrid_k: str = "") -> int:
    queued_count = 0
    with tasks_lock:
        for url, title in items_list:
            config.task_counter += 1
            queued_count += 1
            new_task = {
                "id": config.task_counter,
                "url": url,
                "is_torrent_file": False,
                "title": sanitize_filename(title),
                "has_custom_title": True,
                "debrid_provider": debrid_prov,
                "debrid_key": debrid_k,
                "status": "Queued for Download",
                "parallel_workers": int(workers_val),
                "download_pct": 0.0,
                "download_speed": "--",
                "downloaded_str": "0 MB",
                "total_str": "--",
                "convert_pct": 0.0,
                "raw_size_mb": 0.0,
                "final_size_mb": 0.0,
                "chunk_status": {},
                "error_msg": "",
                "cancelled": False,
                "active_proc": None
            }
            config.tasks.append(new_task)
            threading.Thread(target=run_task_pipeline, args=(new_task,), daemon=True).start()
    return queued_count
