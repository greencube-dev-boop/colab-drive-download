import os
import time
import glob
import shutil
import threading
from typing import Dict, Any, List, Tuple, Optional
import config
from config import (
    RAW_DIR, LOCAL_RAW_DIR, LOCAL_REFINE_DIR, TEMP_DIR,
    REFINED_MOVIES_DIR, REFINED_TV_DIR, REFINED_OTHER_DIR,
    download_semaphore, conversion_semaphore,
    tasks, tasks_lock, sanitize_filename, bypass_trash_delete
)
import database
import metadata_manager
import classifier
from resolvers import resolve_video_source
from downloader import (
    download_hls_stream, download_ytdlp, download_aria2c_or_requests, download_aria2c_torrent
)
from encoder import convert_worker, register_proc, unregister_proc

# Active refining task tracker for cancellation
_active_refine_tasks: Dict[int, Dict[str, Any]] = {}
_refine_lock = threading.Lock()

def cleanup_task_files(task_id: int, task: Optional[Dict[str, Any]] = None):
    """Deletes all local raw, temp, chunks, and intermediate files for a given task."""
    shutil.rmtree(os.path.join(TEMP_DIR, f"chunks_{task_id}"), ignore_errors=True)

    patterns = [
        os.path.join(TEMP_DIR, f"audio_{task_id}.*"),
        os.path.join(TEMP_DIR, f"stitched_{task_id}.mkv"),
        os.path.join(TEMP_DIR, f"concat_{task_id}.txt"),
        os.path.join(TEMP_DIR, f"*_{task_id}*"),
        os.path.join(LOCAL_RAW_DIR, f"task_{task_id}_*"),
        os.path.join(LOCAL_REFINE_DIR, f"refine_{task_id}_*")
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
    """Cancels an active or queued process (download or refining), kills subprocesses, and removes workspace artifacts."""
    found = False

    # 1. Check in-memory active tasks
    with tasks_lock:
        target_task = next((t for t in config.tasks if str(t.get("id")) == str(task_id)), None)
        if target_task:
            found = True
            target_task["cancelled"] = True
            target_task["status"] = "Cancelled"

            # Kill primary and worker procs
            proc = target_task.get("active_proc")
            if proc:
                try:
                    proc.terminate()
                    proc.kill()
                except Exception:
                    pass

            for p in list(target_task.get("active_procs", [])):
                try:
                    p.terminate()
                    p.kill()
                except Exception:
                    pass

            target_task["active_proc"] = None
            target_task["active_procs"] = []
            cleanup_task_files(task_id, target_task)
            config.tasks = [item for item in config.tasks if str(item.get("id")) != str(task_id)]

    # 2. Check active refining daemon tasks
    with _refine_lock:
        refine_task = _active_refine_tasks.get(task_id)
        if refine_task:
            found = True
            refine_task["cancelled"] = True
            refine_task["status"] = "Cancelled"
            proc = refine_task.get("active_proc")
            if proc:
                try:
                    proc.terminate()
                    proc.kill()
                except Exception:
                    pass
            for p in list(refine_task.get("active_procs", [])):
                try:
                    p.terminate()
                    p.kill()
                except Exception:
                    pass
            cleanup_task_files(task_id, refine_task)

    # 3. Cancel in persistent SQLite databases
    database.cancel_queue_item("download_queue", task_id)
    database.cancel_queue_item("refine_queue", task_id)

    return found

def clear_done_tasks():
    """Removes completed, errored, and cancelled tasks from in-memory registry and database tables."""
    with tasks_lock:
        config.tasks = [t for t in config.tasks if t.get("status") not in ["Done", "Error", "Cancelled", "Completed", "Staged"]]
    database.clear_completed("download_queue")
    database.clear_completed("refine_queue")

def download_worker(task: Dict[str, Any]):
    """Downloads video stream or torrent payload directly into local VM workspace."""
    os.makedirs(LOCAL_RAW_DIR, exist_ok=True)

    # 1. Resolve source
    resolved = resolve_video_source(task["url"], task=task)
    source_url = resolved["url"]
    source_type = resolved["type"]
    headers = resolved["headers"]

    # 2. Set title & file extension (preserve user custom title if given)
    if not task.get("has_custom_title") and resolved.get("title"):
        task["title"] = sanitize_filename(resolved["title"])

    ext = ".ts" if source_type == "videm_hls" else ".mp4"
    target_name = f"task_{task['id']}_{sanitize_filename(task['title'])}{ext}"
    task["raw_path"] = os.path.join(LOCAL_RAW_DIR, target_name)

    if task.get("cancelled"):
        return

    # 3. Route to optimal downloader
    if source_type == "local_torrent":
        largest_fp = download_aria2c_torrent(source_url, task)
        if largest_fp and os.path.exists(largest_fp):
            ext = os.path.splitext(largest_fp)[1].lower() or ".mkv"
            dest_path = os.path.join(LOCAL_RAW_DIR, f"task_{task['id']}_{sanitize_filename(task['title'])}{ext}")
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

def run_download_pipeline(task: Dict[str, Any]):
    """
    Stage 1 Execution:
    1. Download video to local Colab workspace
    2. Classify media using AI / Heuristic Regex
    3. Move raw video to Google Drive (drive/downloader/raw/<formatted_name>.<ext>)
    4. Record in drive/downloader/data/metadata.json (status='raw')
    5. Enqueue into persistent SQLite queue (drive/downloader/data/queue.db)
    """
    t_id = task.get("id")
    db_id = task.get("db_id", t_id)
    try:
        if task.get("cancelled"):
            return

        task["status"] = "Queued for Download"
        database.update_item_status("download_queue", db_id, status="pending")

        with download_semaphore:
            if task.get("cancelled"):
                return
            task["status"] = "Downloading"
            database.update_item_status("download_queue", db_id, status="downloading")
            download_worker(task)

        if task.get("cancelled") or not os.path.exists(task.get("raw_path", "")):
            return

        task["status"] = "Classifying & Staging"
        database.update_item_status("download_queue", db_id, status="staging")

        # 1. AI & Heuristic Classification
        media_info = classifier.classify_media(task.get("title", ""))
        media_type = media_info.get("type", "other")
        show_name = media_info.get("show_name", "")
        season = media_info.get("season", 0)
        episode = media_info.get("episode", 0)
        formatted_name = media_info.get("formatted_name", sanitize_filename(task.get("title", "")))

        # 2. Determine raw filename & Drive raw staging path
        raw_ext = os.path.splitext(task["raw_path"])[1].lower() or ".mp4"
        standard_raw_filename = f"{formatted_name}{raw_ext}"
        drive_raw_path = os.path.join(config.RAW_DIR, standard_raw_filename)

        # Avoid collision if same file name already exists in raw
        c = 1
        base_stem, ext = os.path.splitext(standard_raw_filename)
        while os.path.exists(drive_raw_path):
            standard_raw_filename = f"{base_stem}_{c}{ext}"
            drive_raw_path = os.path.join(config.RAW_DIR, standard_raw_filename)
            c += 1

        os.makedirs(config.RAW_DIR, exist_ok=True)
        shutil.move(task["raw_path"], drive_raw_path)
        raw_size_mb = os.path.getsize(drive_raw_path) / (1024 * 1024)

        # 3. Determine expected refined output path in Google Drive
        container_choice = task.get("container_format", "mkv").lower()
        target_ext = ".mp4" if "mp4" in container_choice else ".mkv"
        if media_type == "tv_series":
            season_dir_name = f"Season {season:02d}" if season > 0 else "Season 01"
            clean_show = sanitize_filename(show_name or formatted_name)
            target_refined_path = os.path.join(config.REFINED_TV_DIR, clean_show, season_dir_name, f"{formatted_name}{target_ext}")
        elif media_type == "movie":
            target_refined_path = os.path.join(config.REFINED_MOVIES_DIR, f"{formatted_name}{target_ext}")
        else:
            target_refined_path = os.path.join(config.REFINED_OTHER_DIR, f"{formatted_name}{target_ext}")

        refining_config = {
            "res_preset": task.get("res_preset", "480p"),
            "video_codec": task.get("video_codec", "H.265 / HEVC"),
            "container_format": container_choice,
            "audio_preset": task.get("audio_preset", "Opus 64 kbps"),
            "crf_val": task.get("crf_val", 32),
            "parallel_workers": task.get("parallel_workers", 3)
        }

        # 4. Record in metadata.json
        metadata_manager.record_item(
            media_type=media_type,
            name=formatted_name,
            status="raw",
            raw_path=drive_raw_path,
            refined_path=target_refined_path,
            refining_data=refining_config,
            show_name=show_name,
            season=season,
            episode=episode
        )

        # 5. Enqueue into persistent refine_queue in queue.db
        refine_job_id = database.enqueue_refine({
            "media_type": media_type,
            "show_name": show_name,
            "season": season,
            "episode": episode,
            "title": formatted_name,
            "raw_drive_path": drive_raw_path,
            "target_refined_path": target_refined_path,
            "raw_size_mb": raw_size_mb,
            "refining_config": refining_config
        })

        # 6. Mark Stage 1 completed
        task["status"] = "Staged in Drive"
        task["download_pct"] = 100.0
        database.update_item_status(
            "download_queue",
            db_id,
            status="completed",
            download_pct=100.0,
            raw_path=drive_raw_path
        )

    except Exception as e:
        if not task.get("cancelled"):
            task["status"] = "Error"
            task["error_msg"] = str(e)
            database.update_item_status("download_queue", db_id, status="failed", error_msg=str(e))
    finally:
        if task.get("cancelled"):
            task["status"] = "Cancelled"
            cleanup_task_files(t_id, task)

def _refine_job_worker(job: Dict[str, Any]):
    """
    Stage 2 Execution:
    1. Read task from refine_queue
    2. Copy video from drive/downloader/raw to local workspace
    3. Transcode with chunked NVENC / CPU fallback
    4. Assemble refined video locally
    5. Copy to drive/downloader/refined/<tv_series|movies|other>
    6. Verify final file on Drive > 0 bytes
    7. Bypass Drive Trash Bin: Move raw file to local VM disk, then delete locally
    8. Update metadata.json and queue.db
    """
    job_id = job["id"]
    raw_drive_path = job.get("raw_drive_path", "")
    target_refined_path = job.get("target_refined_path", "")
    cfg = job.get("refining_config", {})

    if not raw_drive_path or not os.path.exists(raw_drive_path):
        err = f"Raw source file not found on Drive: {raw_drive_path}"
        database.update_item_status("refine_queue", job_id, status="failed", error_msg=err)
        return

    os.makedirs(LOCAL_REFINE_DIR, exist_ok=True)
    local_raw_copy = os.path.join(LOCAL_REFINE_DIR, f"refine_{job_id}_{os.path.basename(raw_drive_path)}")

    # Construct execution task context
    refine_task: Dict[str, Any] = {
        "id": job_id,
        "title": job.get("title", "Refined Video"),
        "raw_path": local_raw_copy,
        "res_preset": cfg.get("res_preset", "480p"),
        "video_codec": cfg.get("video_codec", "H.265 / HEVC"),
        "container_format": cfg.get("container_format", "mkv"),
        "audio_preset": cfg.get("audio_preset", "Opus 64 kbps"),
        "crf_val": cfg.get("crf_val", 32),
        "parallel_workers": cfg.get("parallel_workers", 3),
        "cancelled": False,
        "active_proc": None,
        "active_procs": [],
        "convert_pct": 0.0,
        "chunk_status": {}
    }

    with _refine_lock:
        _active_refine_tasks[job_id] = refine_task

    try:
        # Step A: Copy from Drive to local VM ephemeral disk for ultra-fast NVENC access
        database.update_item_status("refine_queue", job_id, status="refining", convert_pct=5.0)
        shutil.copy2(raw_drive_path, local_raw_copy)

        if refine_task.get("cancelled"):
            return

        # Step B: Execute GPU VRAM chunked transcoding
        database.update_item_status("refine_queue", job_id, status="refining", convert_pct=10.0)
        convert_worker(refine_task)

        if refine_task.get("cancelled"):
            return

        temp_output = refine_task.get("temp_path")
        if not temp_output or not os.path.exists(temp_output):
            raise RuntimeError(f"Transcoded output not created at {temp_output}")

        final_size_mb = os.path.getsize(temp_output) / (1024 * 1024)

        # Step C: Move to structured refined destination on Google Drive
        os.makedirs(os.path.dirname(target_refined_path), exist_ok=True)
        shutil.copy2(temp_output, target_refined_path)

        # Step D: Strict verification on Google Drive
        if not os.path.exists(target_refined_path) or os.path.getsize(target_refined_path) == 0:
            raise RuntimeError("Verification failed: Refined file not saved to Google Drive.")

        # Step E: Trash Bin Bypass Deletion of Raw File
        bypass_trash_delete(raw_drive_path)

        # Step F: Update metadata.json & queue.db
        metadata_manager.update_item_status(
            media_type=job.get("media_type", "other"),
            name=job.get("title", ""),
            status="refined",
            refined_path=target_refined_path,
            show_name=job.get("show_name", ""),
            season=job.get("season", 0),
            episode=job.get("episode", 0)
        )
        database.complete_refine_job(job_id, final_size_mb=final_size_mb)

    except Exception as e:
        if not refine_task.get("cancelled"):
            err_msg = str(e)
            print(f"[RefineWorker] Error refining job #{job_id}: {err_msg}")
            database.update_item_status("refine_queue", job_id, status="failed", error_msg=err_msg)
    finally:
        cleanup_task_files(job_id, refine_task)
        with _refine_lock:
            _active_refine_tasks.pop(job_id, None)

def _refine_daemon_loop():
    """Continuous background loop picking and processing jobs from persistent refine_queue."""
    while True:
        if not config.refine_worker_active:
            time.sleep(2.0)
            continue

        acquired = conversion_semaphore.acquire(blocking=False)
        if not acquired:
            time.sleep(1.0)
            continue

        try:
            job = database.claim_next_refine_job()
            if job:
                _refine_job_worker(job)
            else:
                time.sleep(2.0)
        except Exception as e:
            print(f"[RefineDaemon] Exception in loop: {e}")
            time.sleep(2.0)
        finally:
            conversion_semaphore.release()

# Start background refining daemon thread once
_daemon_started = False
def ensure_refine_daemon_running():
    global _daemon_started
    if not _daemon_started:
        _daemon_started = True
        t = threading.Thread(target=_refine_daemon_loop, daemon=True, name="RefineDaemon")
        t.start()


def queue_direct_items(
    items_list: List[Tuple[str, str]],
    workers_val: int = 3,
    debrid_prov: str = "local",
    debrid_k: str = "",
    download_conn: int = 8,
    res_preset: str = "480p",
    v_codec: str = "H.265 / HEVC",
    container: str = "mkv",
    audio_preset: str = "Opus 64 kbps",
    crf_val: int = 32
) -> int:
    """Queues new download requests into persistent SQLite download_queue and starts download pipeline."""
    database.init_db()
    ensure_refine_daemon_running()
    queued_count = 0

    with tasks_lock:
        for url, title in items_list:
            config.task_counter += 1
            t_id = config.task_counter
            queued_count += 1

            refining_cfg = {
                "res_preset": res_preset,
                "video_codec": v_codec,
                "container_format": container.lower(),
                "audio_preset": audio_preset,
                "crf_val": int(crf_val),
                "parallel_workers": int(workers_val)
            }

            # Enqueue to persistent database
            db_id = database.enqueue_download({
                "title": sanitize_filename(title),
                "url": url,
                "source_type": "torrent" if (".torrent" in url.lower() or "magnet:" in url.lower()) else "direct",
                "refining_config": refining_cfg
            })

            new_task = {
                "id": t_id,
                "db_id": db_id,
                "url": url,
                "is_torrent_file": False,
                "title": sanitize_filename(title),
                "has_custom_title": True,
                "debrid_provider": debrid_prov,
                "debrid_key": debrid_k,
                "download_connections": int(download_conn),
                "parallel_workers": int(workers_val),
                "res_preset": res_preset,
                "video_codec": v_codec,
                "container_format": container.lower(),
                "audio_preset": audio_preset,
                "crf_val": int(crf_val),
                "status": "Queued for Download",
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
                "active_proc": None,
                "active_procs": []
            }
            config.tasks.append(new_task)
            threading.Thread(target=run_download_pipeline, args=(new_task,), daemon=True).start()

    return queued_count
