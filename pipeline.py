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
    download_hls_stream, download_ytdlp, download_aria2c_or_requests, download_aria2c_torrent, _kill_proc
)
from encoder import convert_worker, register_proc, unregister_proc

# Active task tracking
_active_refine_tasks: Dict[int, Dict[str, Any]] = {}
_active_download_tasks: Dict[int, Dict[str, Any]] = {}
_refine_lock = threading.Lock()
_dl_lock = threading.Lock()

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

def cancel_task_by_id(task_id: int, table_name: Optional[str] = None, dismiss: bool = False) -> bool:
    """
    Cancels an active or queued process, kills subprocesses, updates DB with reason, and removes temp files.
    If dismiss=True, deletes the item from the queue table.
    """
    found = False
    tables_to_check = [table_name] if table_name in ["download_queue", "refine_queue"] else ["download_queue", "refine_queue"]

    # 1. If explicit dismiss is requested, delete item from queue table
    if dismiss:
        for t_name in tables_to_check:
            item = database.get_queue_item(t_name, task_id)
            if item:
                database.delete_queue_item(t_name, task_id)
                found = True
        with tasks_lock:
            config.tasks = [t for t in config.tasks if str(t.get("id")) != str(task_id) and str(t.get("db_id")) != str(task_id)]
        return found

    # 2. Check in-memory active tasks in config.tasks
    with tasks_lock:
        target_task = next((t for t in config.tasks if str(t.get("id")) == str(task_id) or str(t.get("db_id")) == str(task_id)), None)
        if target_task:
            found = True
            target_task["cancelled"] = True
            target_task["status"] = "Cancelled by user"

            proc = target_task.get("active_proc")
            if proc:
                _kill_proc(proc)

            for p in list(target_task.get("active_procs", [])):
                _kill_proc(p)

            target_task["active_proc"] = None
            target_task["active_procs"] = []
            cleanup_task_files(task_id, target_task)

    # 3. Check active download daemon tasks
    if table_name in [None, "download_queue"]:
        with _dl_lock:
            dl_task = next((v for k, v in _active_download_tasks.items() if str(k) == str(task_id) or str(v.get("id")) == str(task_id)), None)
            if dl_task:
                found = True
                dl_task["cancelled"] = True
                dl_task["status"] = "Cancelled by user"
                proc = dl_task.get("active_proc")
                if proc:
                    _kill_proc(proc)
                for p in list(dl_task.get("active_procs", [])):
                    _kill_proc(p)
                dl_task["active_proc"] = None
                dl_task["active_procs"] = []
                cleanup_task_files(task_id, dl_task)

    # 4. Check active refining daemon tasks
    if table_name in [None, "refine_queue"]:
        with _refine_lock:
            refine_task = next((v for k, v in _active_refine_tasks.items() if str(k) == str(task_id) or str(v.get("id")) == str(task_id)), None)
            if refine_task:
                found = True
                refine_task["cancelled"] = True
                refine_task["status"] = "Cancelled by user"
                proc = refine_task.get("active_proc")
                if proc:
                    _kill_proc(proc)
                for p in list(refine_task.get("active_procs", [])):
                    _kill_proc(p)
                refine_task["active_proc"] = None
                refine_task["active_procs"] = []
                cleanup_task_files(task_id, refine_task)

    # 5. Cancel in persistent SQLite databases with reason
    if table_name in [None, "download_queue"]:
        if database.cancel_queue_item("download_queue", task_id, reason="Cancelled by user"):
            found = True
    if table_name in [None, "refine_queue"]:
        if database.cancel_queue_item("refine_queue", task_id, reason="Cancelled by user"):
            found = True

    return found

def clear_done_tasks():
    """Removes completed, errored, and cancelled tasks from in-memory registry and database tables."""
    with tasks_lock:
        config.tasks = [t for t in config.tasks if t.get("status") not in ["Done", "Error", "Cancelled", "Completed", "Staged", "Cancelled by user"]]
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

    if not task.get("has_custom_title") and not task.get("torrent_file_index") and not task.get("target_file_index") and resolved.get("title"):
        task["title"] = sanitize_filename(resolved["title"])

    ext = ".ts" if source_type == "videm_hls" else ".mp4"
    target_name = f"task_{task['id']}_{sanitize_filename(task['title'])}{ext}"
    task["raw_path"] = os.path.join(LOCAL_RAW_DIR, target_name)

    if task.get("cancelled"):
        return

    # 2. Route to optimal downloader
    if source_type == "local_torrent" or task.get("is_torrent_file") or "magnet:?" in source_url:
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
    Stage 1 Execution with Automated Deduplication:
    1. Pre-download check: Check if media is already in Drive (raw or refined)
    2. Download video to local Colab workspace
    3. Post-download check: Re-verify actual file against Drive
    4. Move raw video to Google Drive (drive/downloader/raw/<formatted_name>.<ext>)
    5. Record in metadata.json and prune any pending duplicates in download_queue
    6. Enqueue into persistent SQLite queue (drive/downloader/data/queue.db)
    """
    t_id = task.get("id")
    db_id = task.get("db_id", t_id)
    try:
        if task.get("cancelled"):
            return

        # -------------------------------------------------------------
        # DEDUPLICATION CHECK 1 (Pre-Download)
        # -------------------------------------------------------------
        pre_info = classifier.classify_media(task.get("title", ""))
        exists, st_exists, existing_path = metadata_manager.check_media_exists(pre_info)
        if exists:
            msg = f"Skipped: Already in Drive as {st_exists} ({os.path.basename(existing_path)})"
            print(f"[Deduplication] Task #{t_id} '{task.get('title')}' -> {msg}")
            task["status"] = msg
            task["cancelled"] = True
            task["download_pct"] = 100.0
            database.update_item_status("download_queue", db_id, status="cancelled", error_msg=msg)
            return

        task["status"] = "Downloading"
        database.update_item_status("download_queue", db_id, status="downloading")
        download_worker(task)

        if task.get("cancelled"):
            return

        if not os.path.exists(task.get("raw_path", "")):
            raise RuntimeError(f"Download output file does not exist: {task.get('raw_path')}")

        task["status"] = "Classifying & Staging"
        database.update_item_status("download_queue", db_id, status="staging")

        # -------------------------------------------------------------
        # DEDUPLICATION CHECK 2 (Post-Download, using actual downloaded media)
        # -------------------------------------------------------------
        downloaded_name = os.path.basename(task["raw_path"])
        media_info = classifier.classify_media(task.get("title", "") or downloaded_name)
        exists, st_exists, existing_path = metadata_manager.check_media_exists(media_info)
        if exists:
            print(f"[Deduplication] Task #{t_id} downloaded duplicate detected. Purging local copy -> {task['raw_path']}")
            try:
                if os.path.exists(task["raw_path"]):
                    os.remove(task["raw_path"])
            except Exception:
                pass
            msg = f"Cancelled: Duplicate of existing media in Drive ({st_exists})"
            task["status"] = msg
            task["cancelled"] = True
            database.update_item_status("download_queue", db_id, status="cancelled", error_msg=msg)
            return

        media_type = media_info.get("type", "other")
        show_name = media_info.get("show_name", "")
        season = media_info.get("season", 0)
        episode = media_info.get("episode", 0)
        formatted_name = media_info.get("formatted_name", sanitize_filename(task.get("title", "")))

        # Determine raw filename & Drive raw staging path
        raw_ext = os.path.splitext(task["raw_path"])[1].lower() or ".mp4"
        standard_raw_filename = f"{formatted_name}{raw_ext}"
        drive_raw_path = os.path.join(config.RAW_DIR, standard_raw_filename)

        c = 1
        base_stem, ext = os.path.splitext(standard_raw_filename)
        while os.path.exists(drive_raw_path):
            standard_raw_filename = f"{base_stem}_{c}{ext}"
            drive_raw_path = os.path.join(config.RAW_DIR, standard_raw_filename)
            c += 1

        os.makedirs(config.RAW_DIR, exist_ok=True)
        shutil.move(task["raw_path"], drive_raw_path)
        raw_size_mb = os.path.getsize(drive_raw_path) / (1024 * 1024)

        # Determine expected refined output path in Google Drive
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

        # Record in metadata.json
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

        # -------------------------------------------------------------
        # DEDUPLICATION CHECK 3 (Prune pending queue duplicates)
        # -------------------------------------------------------------
        pruned_count = metadata_manager.prune_pending_duplicates(media_info)
        if pruned_count > 0:
            print(f"[Deduplication] Pruned {pruned_count} pending duplicate(s) from download queue.")

        # Enqueue into persistent refine_queue in queue.db
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

        task["status"] = "Staged in Drive"
        task["download_pct"] = 100.0
        database.update_item_status(
            "download_queue",
            db_id,
            status="completed",
            download_pct=100.0,
            raw_path=drive_raw_path,
            error_msg=""
        )

    except Exception as e:
        if not task.get("cancelled"):
            err_msg = str(e)
            print(f"[DownloadWorker] Error on task #{t_id}: {err_msg}")
            task["status"] = "Error"
            task["error_msg"] = err_msg
            database.update_item_status("download_queue", db_id, status="failed", error_msg=err_msg)
    finally:
        if task.get("cancelled"):
            if not str(task.get("status", "")).startswith("Cancelled") and not str(task.get("status", "")).startswith("Skipped"):
                task["status"] = "Cancelled"
            database.update_item_status("download_queue", db_id, status="cancelled", error_msg=task.get("status") or "Cancelled by user")
            cleanup_task_files(t_id, task)

# ---------------------------------------------------------------------
# Dedicated Download Worker & Queue-Driven Daemon
# ---------------------------------------------------------------------
def _download_job_worker(job: Dict[str, Any]):
    """Processes a claimed download job and releases download semaphore upon completion."""
    job_id = job["id"]
    cfg = job.get("refining_config", {})

    task: Dict[str, Any] = {
        "id": job_id,
        "db_id": job_id,
        "title": job.get("title", "Download"),
        "url": job.get("url", ""),
        "source_type": job.get("source_type", "direct"),
        "has_custom_title": bool(job.get("has_custom_title", False) or cfg.get("has_custom_title", False) or job.get("torrent_file_index") or cfg.get("torrent_file_index")),
        "torrent_cached_path": cfg.get("torrent_cached_path") or job.get("torrent_cached_path"),
        "raw_path": "",
        "res_preset": cfg.get("res_preset", "480p"),
        "video_codec": cfg.get("video_codec", "H.265 / HEVC"),
        "container_format": cfg.get("container_format", "mkv"),
        "audio_preset": cfg.get("audio_preset", "Opus 64 kbps"),
        "crf_val": cfg.get("crf_val", 32),
        "parallel_workers": cfg.get("parallel_workers", 3),
        "download_connections": cfg.get("download_connections", 8),
        "torrent_file_index": job.get("torrent_file_index") or cfg.get("torrent_file_index"),
        "target_file_index": job.get("target_file_index") or cfg.get("target_file_index"),
        "torrent_filename": job.get("torrent_filename") or cfg.get("torrent_filename"),
        "rel_folder": job.get("rel_folder", "Root") or cfg.get("rel_folder", "Root"),
        "cancelled": False,
        "active_proc": None,
        "active_procs": [],
        "download_pct": 0.0,
        "download_speed": "--",
        "downloaded_str": "0 MB",
        "total_str": "--"
    }

    with _dl_lock:
        _active_download_tasks[job_id] = task

    with tasks_lock:
        config.tasks = [t for t in config.tasks if str(t.get("id")) != str(job_id) and str(t.get("db_id")) != str(job_id)]
        config.tasks.append(task)

    try:
        run_download_pipeline(task)
    finally:
        with _dl_lock:
            _active_download_tasks.pop(job_id, None)
        download_semaphore.release()

def _download_daemon_loop():
    """Continuous background loop strictly executing download jobs in order_idx ASC sequence."""
    while True:
        acquired = download_semaphore.acquire(blocking=False)
        if not acquired:
            time.sleep(1.0)
            continue

        try:
            job = database.claim_next_download_job()
            if job:
                t = threading.Thread(target=_download_job_worker, args=(job,), daemon=True)
                t.start()
            else:
                download_semaphore.release()
                time.sleep(1.5)
        except Exception as e:
            print(f"[DownloadDaemon] Loop exception: {e}")
            download_semaphore.release()
            time.sleep(2.0)

# ---------------------------------------------------------------------
# Dedicated Refining Worker & Queue-Driven Daemon
# ---------------------------------------------------------------------
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
        "convert_pct": 5.0,
        "chunk_status": {}
    }

    with _refine_lock:
        _active_refine_tasks[job_id] = refine_task

    try:
        database.update_item_status("refine_queue", job_id, status="refining", convert_pct=5.0)
        shutil.copy2(raw_drive_path, local_raw_copy)

        if refine_task.get("cancelled"):
            return

        database.update_item_status("refine_queue", job_id, status="refining", convert_pct=10.0)
        convert_worker(refine_task)

        if refine_task.get("cancelled"):
            return

        temp_output = refine_task.get("temp_path")
        if not temp_output or not os.path.exists(temp_output):
            raise RuntimeError(f"Transcoded output not created at {temp_output}")

        final_size_mb = os.path.getsize(temp_output) / (1024 * 1024)

        os.makedirs(os.path.dirname(target_refined_path), exist_ok=True)
        shutil.copy2(temp_output, target_refined_path)

        if not os.path.exists(target_refined_path) or os.path.getsize(target_refined_path) == 0:
            raise RuntimeError("Verification failed: Refined file not saved to Google Drive.")

        # Bypass Google Drive Trash Bin
        bypass_trash_delete(raw_drive_path)

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
    """Continuous background loop strictly executing refining jobs in order_idx ASC sequence."""
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

# Start background daemons
_daemons_started = False
def ensure_daemons_running():
    global _daemons_started
    if not _daemons_started:
        _daemons_started = True
        threading.Thread(target=_download_daemon_loop, daemon=True, name="DownloadDaemon").start()
        threading.Thread(target=_refine_daemon_loop, daemon=True, name="RefineDaemon").start()

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
    """Queues new download requests into persistent SQLite download_queue with auto torrent splitting."""
    database.init_db()
    ensure_daemons_running()
    queued_count = 0

    for url, title in items_list:
        clean_url = (url or "").strip()
        if not clean_url:
            continue

        is_torrent = (".torrent" in clean_url.lower() or "magnet:?" in clean_url.lower())

        # Auto-Inspect Multi-Video Torrents when added without inspecting
        if is_torrent:
            try:
                from torrent_parser import fetch_torrent_metadata
                meta = fetch_torrent_metadata(clean_url)
                vids = [f for f in meta.get("files", []) if f.get("is_video")]
                if len(vids) >= 1:
                    print(f"[TorrentAutoSplit] Torrent '{meta.get('name')}' contains {len(vids)} video file(s). Enqueueing video(s)...")
                    for vf in vids:
                        refining_cfg = {
                            "res_preset": res_preset,
                            "video_codec": v_codec,
                            "container_format": container.lower(),
                            "audio_preset": audio_preset,
                            "crf_val": int(crf_val),
                            "parallel_workers": int(workers_val),
                            "download_connections": int(download_conn),
                            "torrent_file_index": vf.get("index"),
                            "target_file_index": vf.get("index"),
                            "torrent_filename": vf.get("filename"),
                            "rel_folder": vf.get("folder", "Root"),
                            "torrent_cached_path": meta.get("cached_file_path"),
                            "has_custom_title": True
                        }
                        database.enqueue_download({
                            "title": sanitize_filename(vf.get("filename_clean")),
                            "url": meta.get("cached_file_path") or clean_url,
                            "source_type": "torrent",
                            "refining_config": refining_cfg
                        })
                        queued_count += 1
                    continue
            except Exception as e:
                print(f"[TorrentAutoSplit] Notice: {e}. Falling back to standard queue.")

        refining_cfg = {
            "res_preset": res_preset,
            "video_codec": v_codec,
            "container_format": container.lower(),
            "audio_preset": audio_preset,
            "crf_val": int(crf_val),
            "parallel_workers": int(workers_val),
            "download_connections": int(download_conn)
        }

        database.enqueue_download({
            "title": sanitize_filename(title) if title else "Video Download",
            "url": clean_url,
            "source_type": "torrent" if is_torrent else "direct",
            "refining_config": refining_cfg
        })
        queued_count += 1

    return queued_count
