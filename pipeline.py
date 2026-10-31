import os
import time
import glob
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
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
import logger
from downloader import (
    download_hls_stream, download_ytdlp, download_aria2c_or_requests, download_aria2c_torrent, _kill_proc
)
from encoder import convert_worker, register_proc, unregister_proc

# Active task tracking
_active_refine_tasks: Dict[int, Dict[str, Any]] = {}
_active_download_tasks: Dict[int, Dict[str, Any]] = {}
_refine_lock = threading.Lock()
_dl_lock = threading.Lock()
_assembly_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="CPUAssembly")

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
        for key in ["temp_path"]:
            p = task.get(key)
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

        # Safely clean up raw_path ONLY if it is a local scratch download file
        # NEVER delete files residing in Google Drive raw or storage directories!
        raw_p = task.get("raw_path")
        if raw_p and os.path.exists(raw_p):
            is_drive_file = False
            raw_dir = getattr(config, "RAW_DIR", "")
            drive_dir = getattr(config, "DRIVE_DIR", "")
            if raw_dir and os.path.abspath(raw_p).startswith(os.path.abspath(raw_dir)):
                is_drive_file = True
            elif drive_dir and os.path.abspath(raw_p).startswith(os.path.abspath(drive_dir)):
                is_drive_file = True
            if not is_drive_file:
                try:
                    os.remove(raw_p)
                except Exception:
                    pass

        p_url = task.get("url")
        if p_url and task.get("is_torrent_file") and os.path.exists(p_url):
            try:
                os.remove(p_url)
            except Exception:
                pass

def cleanup_workspace(force: bool = False) -> Dict[str, Any]:
    """
    Cleans up temporary, orphaned, and stale files from the Colab local workspace and Drive staging.
    - Removes finished/orphaned chunk staging directories (drive/downloader/staging/chunks/job_X)
    - Removes orphaned stitched/concat/audio/segment files in LOCAL_WORKSPACE
    - Removes stale temp files in LOCAL_RAW_DIR, LOCAL_REFINE_DIR, TRASH_STAGING_DIR
    """
    freed_bytes = 0
    cleaned_staging_folders = 0
    cleaned_local_files = 0

    # 1. Gather all currently active job IDs across download, refine, and cluster
    active_ids = set()
    with _refine_lock:
        active_ids.update(str(k) for k in _active_refine_tasks.keys())
    with _dl_lock:
        active_ids.update(str(k) for k in _active_download_tasks.keys())
    with tasks_lock:
        active_ids.update(str(t.get("id")) for t in config.tasks if t.get("id"))
    try:
        import cluster_manager
        with cluster_manager._cluster_lock:
            active_ids.update(str(k) for k in cluster_manager._active_cluster_jobs.keys())
    except Exception:
        pass

    now = time.time()

    # 2. Clean orphaned chunk staging folders on Google Drive
    chunks_staging = getattr(config, "CHUNKS_STAGING_DIR", "")
    if chunks_staging and os.path.exists(chunks_staging):
        try:
            for item in os.listdir(chunks_staging):
                full_p = os.path.join(chunks_staging, item)
                if os.path.isdir(full_p) and item.startswith("job_"):
                    jid = item[4:]
                    if jid not in active_ids or force:
                        try:
                            if not force:
                                db_item = database.get_queue_item("refine_queue", int(jid)) if jid.isdigit() else None
                                if db_item and db_item.get("status") not in ["completed", "failed", "cancelled"]:
                                    continue
                            dir_size = 0
                            for dp, _, fns in os.walk(full_p):
                                for fn in fns:
                                    fp = os.path.join(dp, fn)
                                    if os.path.exists(fp):
                                        dir_size += os.path.getsize(fp)
                            shutil.rmtree(full_p, ignore_errors=True)
                            freed_bytes += dir_size
                            cleaned_staging_folders += 1
                        except Exception:
                            pass
        except Exception:
            pass

    # 3. Clean ephemeral local scratch directories
    local_ws = getattr(config, "LOCAL_WORKSPACE", "")
    if local_ws and os.path.exists(local_ws):
        try:
            for root, dirs, files in os.walk(local_ws):
                for fn in files:
                    if fn.endswith(".db") or fn.endswith(".db-journal") or fn.endswith(".json"):
                        continue
                    fp = os.path.join(root, fn)
                    try:
                        mtime = os.path.getmtime(fp)
                        age_sec = now - mtime
                        is_active_job_file = any(f"_{aid}." in fn or f"_{aid}_" in fn or f"job_{aid}" in fp for aid in active_ids)
                        should_delete = False
                        if force and not is_active_job_file:
                            should_delete = True
                        elif not is_active_job_file and age_sec > 1200:
                            should_delete = True
                        elif age_sec > 7200:
                            should_delete = True

                        if should_delete and os.path.exists(fp):
                            f_size = os.path.getsize(fp)
                            os.remove(fp)
                            freed_bytes += f_size
                            cleaned_local_files += 1
                    except Exception:
                        pass
        except Exception:
            pass

    # 4. Clean trash staging directory
    trash_dir = getattr(config, "TRASH_STAGING_DIR", "")
    if trash_dir and os.path.exists(trash_dir):
        try:
            for item in os.listdir(trash_dir):
                fp = os.path.join(trash_dir, item)
                try:
                    if os.path.isfile(fp):
                        freed_bytes += os.path.getsize(fp)
                        os.remove(fp)
                        cleaned_local_files += 1
                    elif os.path.isdir(fp):
                        shutil.rmtree(fp, ignore_errors=True)
                except Exception:
                    pass
        except Exception:
            pass

    freed_mb = freed_bytes / (1024 * 1024)
    free_gb = 0.0
    try:
        _, _, free_b = shutil.disk_usage(local_ws or "/content")
        free_gb = free_b / (1024 ** 3)
    except Exception:
        pass

    if freed_mb > 0 or force:
        logger.notify("SYSTEM", f"🧹 Workspace cleanup: Reclaimed {freed_mb:.1f} MB. Available local disk: {free_gb:.1f} GB.")

    return {
        "freed_mb": freed_mb,
        "free_gb": free_gb,
        "cleaned_staging_folders": cleaned_staging_folders,
        "cleaned_local_files": cleaned_local_files
    }

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
    try:
        cleanup_workspace(force=False)
    except Exception:
        pass

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
            logger.warn("DOWNLOAD", f"⚠️ Skipped Task #{t_id} ('{task.get('title')}'): Already in Drive as {st_exists} ({os.path.basename(existing_path)})")
            task["status"] = msg
            task["cancelled"] = True
            task["download_pct"] = 100.0
            database.update_item_status("download_queue", db_id, status="cancelled", error_msg=msg)
            return

        task["status"] = "Downloading"
        logger.notify("DOWNLOAD", f"🚀 Started downloading Task #{t_id}: '{task.get('title')}'")
        database.update_item_status("download_queue", db_id, status="downloading")
        download_worker(task)

        # 1. Check for Watchdog start timeout (90s)
        if task.get("timed_out_start"):
            requeued, retry_num = database.requeue_download_for_retry(db_id, max_retries=config.MAX_DOWNLOAD_START_RETRIES)
            if requeued:
                msg = f"Start timeout (90s): Moved to end of queue for retry ({retry_num}/3)"
                logger.warn("DOWNLOAD", f"⏳ Start timeout for Task #{t_id} ('{task.get('title')}'): Requeued for retry ({retry_num}/3)")
                task["status"] = msg
                task["cancelled"] = True
            else:
                msg = "Timeout: Failed to start after 3 attempts (no active swarm/seeds)"
                logger.error("DOWNLOAD", f"❌ Failed to start Task #{t_id} ('{task.get('title')}'): Swarm inactive after 3 attempts.")
                task["status"] = msg
                task["error_msg"] = msg
                task["cancelled"] = True
            cleanup_task_files(t_id, task)
            return

        # 2. Check for Low Speed demote (< 100 KB/s for 120s)
        if task.get("demote_low_speed"):
            msg = "Demoted to Low-Speed Queue (< 100 KB/s for 120s)"
            logger.warn("DOWNLOAD", f"🐢 Demoted Task #{t_id} ('{task.get('title')}') to Low-Speed Queue (< 100 KB/s for 120s)")
            database.demote_to_low_speed_queue(db_id, reason=msg)
            task["status"] = msg
            task["cancelled"] = True
            cleanup_task_files(t_id, task)
            return

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
            logger.warn("DOWNLOAD", f"⚠️ Purging duplicate download Task #{t_id}: Already exists in Drive ({st_exists})")
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
        year = media_info.get("year")
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
        invalidate_unqueued_cache()
        raw_size_mb = os.path.getsize(drive_raw_path) / (1024 * 1024)
        logger.success("DOWNLOAD", f"✅ Download complete: '{standard_raw_filename}' ({raw_size_mb:.1f} MB). Staged in Drive raw folder: {drive_raw_path}")

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
            episode=episode,
            year=year
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
            "year": year,
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
            logger.error("DOWNLOAD", f"❌ Download error on Task #{t_id} ('{task.get('title')}'): {err_msg}")
            task["status"] = "Error"
            task["error_msg"] = err_msg
            database.update_item_status("download_queue", db_id, status="failed", error_msg=err_msg)
    finally:
        if task.get("cancelled") and not task.get("timed_out_start") and not task.get("demote_low_speed"):
            if not str(task.get("status", "")).startswith("Cancelled") and not str(task.get("status", "")).startswith("Skipped"):
                task["status"] = "Cancelled"
            database.update_item_status("download_queue", db_id, status="cancelled", error_msg=task.get("status") or "Cancelled by user")
            cleanup_task_files(t_id, task)

# ---------------------------------------------------------------------
# Dedicated Download Worker & Queue-Driven Daemon
# ---------------------------------------------------------------------
def _download_job_worker(job: Dict[str, Any], is_low_speed: bool = False):
    """Processes a claimed download job and releases download semaphore upon completion."""
    job_id = job["id"]
    cfg = job.get("refining_config", {})
    job_is_low = bool(job.get("is_low_speed") or is_low_speed)

    task: Dict[str, Any] = {
        "id": job_id,
        "db_id": job_id,
        "title": job.get("title", "Download"),
        "url": job.get("url", ""),
        "source_type": job.get("source_type", "direct"),
        "is_low_speed": job_is_low,
        "retry_count": job.get("retry_count", 0),
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
        if job_is_low:
            if getattr(config, "MAX_LOW_SPEED_DOWNLOAD_WORKERS", 0) > 0 and getattr(config, "low_speed_download_semaphore", None) is not None:
                config.low_speed_download_semaphore.release()
        else:
            config.download_semaphore.release()

def _download_daemon_loop():
    """Continuous background loop strictly executing high-speed download jobs in order_idx ASC sequence."""
    while True:
        if not getattr(config, "download_worker_active", True):
            time.sleep(0.5)
            continue
        acquired = config.download_semaphore.acquire(blocking=False)
        if not acquired:
            time.sleep(1.0)
            continue

        try:
            job = database.claim_next_download_job(is_low_speed=False)
            if job:
                t = threading.Thread(target=_download_job_worker, args=(job, False), daemon=True)
                t.start()
            else:
                config.download_semaphore.release()
                time.sleep(1.5)
        except Exception as e:
            print(f"[DownloadDaemon] Loop exception: {e}")
            config.download_semaphore.release()
            time.sleep(2.0)

def _low_speed_download_daemon_loop():
    """Continuous background loop executing slow-lane trickle downloads with unlimited concurrency without blocking high-speed slots."""
    while True:
        if not getattr(config, "download_worker_active", True):
            time.sleep(0.5)
            continue

        limit = getattr(config, "MAX_LOW_SPEED_DOWNLOAD_WORKERS", 0)
        has_limit = (limit > 0 and getattr(config, "low_speed_download_semaphore", None) is not None)
        if has_limit:
            acquired = config.low_speed_download_semaphore.acquire(blocking=False)
            if not acquired:
                time.sleep(1.0)
                continue

        try:
            job = database.claim_next_download_job(is_low_speed=True)
            if job:
                t = threading.Thread(target=_download_job_worker, args=(job, True), daemon=True)
                t.start()
                time.sleep(0.1)
            else:
                if has_limit:
                    config.low_speed_download_semaphore.release()
                time.sleep(1.5)
        except Exception as e:
            print(f"[LowSpeedDaemon] Loop exception: {e}")
            if has_limit:
                config.low_speed_download_semaphore.release()
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

    refine_task: Dict[str, Any] = {
        "id": job_id,
        "title": job.get("title", "Refined Video"),
        "raw_path": raw_drive_path,
        "raw_drive_path": raw_drive_path,
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

    semaphore_released = [False]
    assembly_dispatched = [False]

    def _release_conversion_slot():
        if not semaphore_released[0]:
            semaphore_released[0] = True
            config.conversion_semaphore.release()
            logger.notify("REFINER", f"⚡ GPU Transcoding phase completed for Job #{job_id}. Slot freed: pre-chunking next queued video immediately.")

    try:
        logger.notify("REFINER", f"🎬 Starting Job #{job_id}: '{job.get('title')}' -> Target: {cfg.get('res_preset', '480p')} {cfg.get('video_codec', 'H.265 / HEVC')}")
        database.update_item_status("refine_queue", job_id, status="refining", convert_pct=0.0)

        if refine_task.get("cancelled"):
            return

        refine_task["_async_assembly"] = True
        try:
            convert_worker(refine_task, on_transcode_complete=_release_conversion_slot)
        except TypeError:
            convert_worker(refine_task)
            _release_conversion_slot()

        if refine_task.get("cancelled"):
            return

        # GPU Transcoding completed. Immediately release semaphore permit for upcoming video chunking!
        _release_conversion_slot()

        # Hand off CPU concatenation, audio muxing, and Google Drive upload to asynchronous worker pool
        assembly_dispatched[0] = True
        _assembly_executor.submit(_async_assemble_and_upload, job, refine_task, target_refined_path, raw_drive_path)

    except Exception as e:
        if not refine_task.get("cancelled"):
            err_msg = str(e)
            logger.error("REFINER", f"❌ Error refining Job #{job_id} ('{job.get('title')}'): {err_msg}")
            database.update_item_status("refine_queue", job_id, status="failed", error_msg=err_msg)
    finally:
        # If assembly was not dispatched (e.g. error or cancelled), clean up now; otherwise assembly worker cleans up
        if not assembly_dispatched[0]:
            cleanup_task_files(job_id, refine_task)
            with _refine_lock:
                _active_refine_tasks.pop(job_id, None)
            if not semaphore_released[0]:
                semaphore_released[0] = True
                config.conversion_semaphore.release()
            try:
                cleanup_workspace(force=False)
            except Exception:
                pass

def _async_assemble_and_upload(job: Dict[str, Any], refine_task: Dict[str, Any], target_refined_path: str, raw_drive_path: str):
    """
    Decoupled Asynchronous CPU Assembly Worker:
    Concatenates finished video segments, muxes continuous audio, copies to Google Drive,
    and updates metadata while the GPU cluster continues transcoding subsequent videos.
    """
    job_id = job["id"]
    try:
        database.update_item_status("refine_queue", job_id, status="assembling")
        logger.notify("REFINER", f"🧩 [CPU Assembly] Starting concatenation & muxing for Job #{job_id} ('{job.get('title')}') in background...")

        from encoder import assemble_video_locally
        assemble_video_locally(
            refine_task,
            refine_task.get("_encoded_chunks", []),
            refine_task.get("_audio_thread"),
            refine_task.get("_audio_path", ""),
            refine_task.get("_task_chunks_dir", ""),
            refine_task.get("_res_tag", "480p")
        )

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
            episode=job.get("episode", 0),
            year=job.get("year")
        )
        database.complete_refine_job(job_id, final_size_mb=final_size_mb)
        logger.success("REFINER", f"🎉 Finished Job #{job_id}! Saved complete refined video to Drive: '{os.path.basename(target_refined_path)}' ({final_size_mb:.1f} MB) -> {target_refined_path}. Chunk staging cleared.")

    except Exception as e:
        if not refine_task.get("cancelled"):
            err_msg = str(e)
            logger.error("REFINER", f"❌ Error assembling Job #{job_id} ('{job.get('title')}'): {err_msg}")
            database.update_item_status("refine_queue", job_id, status="failed", error_msg=err_msg)
    finally:
        cleanup_task_files(job_id, refine_task)
        with _refine_lock:
            _active_refine_tasks.pop(job_id, None)
        try:
            cleanup_workspace(force=False)
        except Exception:
            pass

def _refine_daemon_loop():
    """Continuous background loop executing refining jobs concurrently with dynamic lookahead up to MAX_ACTIVE_CLUSTER_JOBS."""
    while True:
        if not config.refine_worker_active:
            time.sleep(2.0)
            continue

        try:
            import cluster_manager
            # Dynamic slot-aware lookahead:
            # If the cluster is already active, only pre-chunk the next video if pending chunks < available GPU slots
            if cluster_manager.get_active_cluster_jobs_count() > 0:
                if not cluster_manager.should_prechunk_next_video():
                    time.sleep(1.0)
                    continue
        except Exception:
            pass

        acquired = config.conversion_semaphore.acquire(blocking=False)
        if not acquired:
            time.sleep(1.0)
            continue

        try:
            job = database.claim_next_refine_job()
            if job:
                t = threading.Thread(target=_refine_job_worker, args=(job,), daemon=True, name=f"RefineWorker_{job['id']}")
                t.start()
            else:
                config.conversion_semaphore.release()
                time.sleep(2.0)
        except Exception as e:
            print(f"[RefineDaemon] Exception in loop: {e}")
            config.conversion_semaphore.release()
            time.sleep(2.0)

# Start background daemons
_daemons_started = False
def ensure_daemons_running():
    global _daemons_started
    if not _daemons_started:
        _daemons_started = True
        try:
            cleanup_workspace(force=False)
        except Exception:
            pass
        threading.Thread(target=_download_daemon_loop, daemon=True, name="DownloadDaemon").start()
        threading.Thread(target=_low_speed_download_daemon_loop, daemon=True, name="LowSpeedDownloadDaemon").start()
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

# ---------------------------------------------------------------------
# Un-queued Raw Drive Files Management
# ---------------------------------------------------------------------
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.webm', '.flv', '.wmv', '.m4v', '.ts'}

_unqueued_cache: Optional[List[Dict[str, Any]]] = None
_unqueued_last_mtime: float = -1.0
_unqueued_lock = threading.Lock()

def invalidate_unqueued_cache():
    """Invalidates the in-memory unqueued raw files cache."""
    global _unqueued_cache, _unqueued_last_mtime
    with _unqueued_lock:
        _unqueued_cache = None
        _unqueued_last_mtime = -1.0

def get_unqueued_raw_files(force_refresh: bool = False) -> List[Dict[str, Any]]:
    """
    Scans RAW_DIR for video files not currently in refine_queue with status 'pending' or 'refining'.
    Caches based on directory modification time to avoid repetitive file statting when directory hasn't changed.
    """
    global _unqueued_cache, _unqueued_last_mtime

    raw_dir = getattr(config, "RAW_DIR", "")
    if not raw_dir or not os.path.exists(raw_dir):
        with _unqueued_lock:
            _unqueued_cache = []
            _unqueued_last_mtime = -1.0
        return []

    try:
        dir_mtime = os.path.getmtime(raw_dir)
    except Exception:
        dir_mtime = time.time()

    with _unqueued_lock:
        if not force_refresh and _unqueued_cache is not None and dir_mtime == _unqueued_last_mtime:
            return list(_unqueued_cache)

    try:
        refine_items = database.get_refine_queue()
    except Exception:
        refine_items = []

    active_or_pending = [j for j in refine_items if j.get("status") in ["pending", "refining"]]
    queued_keys = set()
    for j in active_or_pending:
        raw_p = j.get("raw_drive_path", "")
        if raw_p:
            queued_keys.add(os.path.basename(raw_p).lower())
            queued_keys.add(os.path.splitext(os.path.basename(raw_p))[0].lower())
        title = j.get("title", "")
        if title:
            queued_keys.add(title.lower())

    unqueued = []
    try:
        for fname in os.listdir(raw_dir):
            full_path = os.path.join(raw_dir, fname)
            if not os.path.isfile(full_path):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext not in VIDEO_EXTENSIONS:
                continue

            stem = os.path.splitext(fname)[0].lower()
            if fname.lower() in queued_keys or stem in queued_keys:
                continue

            try:
                sz_mb = os.path.getsize(full_path) / (1024 * 1024)
            except Exception:
                sz_mb = 0.0

            unqueued.append({
                "filename": fname,
                "path": full_path,
                "size_mb": sz_mb,
                "formatted_size": f"{sz_mb:.1f} MB" if sz_mb < 1024 else f"{sz_mb/1024:.2f} GB"
            })
    except Exception as e:
        print(f"[UnqueuedScan] Error reading raw directory: {e}")

    res = sorted(unqueued, key=lambda x: x["filename"].lower())
    with _unqueued_lock:
        _unqueued_cache = res
        _unqueued_last_mtime = dir_mtime
    return res

def enqueue_unqueued_raw_file(filename: str, refining_config: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """Classifies an unqueued raw file, records in metadata.json, and pushes into refine_queue."""
    raw_dir = getattr(config, "RAW_DIR", "")
    full_path = os.path.join(raw_dir, filename)
    if not os.path.exists(full_path):
        return None

    raw_size_mb = os.path.getsize(full_path) / (1024 * 1024)
    media_info = classifier.classify_media(filename)
    media_type = media_info.get("type", "other")
    show_name = media_info.get("show_name", "")
    season = media_info.get("season", 0)
    episode = media_info.get("episode", 0)
    year = media_info.get("year")
    formatted_name = media_info.get("formatted_name", sanitize_filename(os.path.splitext(filename)[0]))

    cfg = refining_config or {
        "res_preset": "480p",
        "video_codec": "H.265 / HEVC",
        "container_format": "mkv",
        "audio_preset": "Opus 64 kbps",
        "crf_val": 32,
        "parallel_workers": 3
    }
    target_ext = f".{cfg.get('container_format', 'mkv').lower()}"

    if media_type == "movie":
        target_refined_path = os.path.join(config.REFINED_MOVIES_DIR, f"{formatted_name}{target_ext}")
    elif media_type == "tv_series":
        target_refined_path = os.path.join(config.REFINED_TV_DIR, show_name, f"Season {season:02d}", f"{formatted_name}{target_ext}")
    else:
        target_refined_path = os.path.join(config.REFINED_OTHER_DIR, f"{formatted_name}{target_ext}")

    metadata_manager.record_item(
        media_type=media_type,
        name=formatted_name,
        status="raw",
        raw_path=full_path,
        refined_path=target_refined_path,
        refining_data=cfg,
        show_name=show_name,
        season=season,
        episode=episode,
        year=year
    )

    refine_job_id = database.enqueue_refine({
        "media_type": media_type,
        "show_name": show_name,
        "season": season,
        "episode": episode,
        "year": year,
        "title": formatted_name,
        "raw_drive_path": full_path,
        "target_refined_path": target_refined_path,
        "raw_size_mb": raw_size_mb,
        "refining_config": cfg
    })
    invalidate_unqueued_cache()
    print(f"[UnqueuedAction] Enqueued '{filename}' as refine job #{refine_job_id} ({formatted_name})")
    return refine_job_id

def delete_unqueued_raw_file(filename: str) -> bool:
    """Permanently deletes an unqueued raw file from Drive using bypass_trash_delete."""
    raw_dir = getattr(config, "RAW_DIR", "")
    full_path = os.path.join(raw_dir, filename)
    if not os.path.exists(full_path):
        return False
    success = config.bypass_trash_delete(full_path)
    invalidate_unqueued_cache()
    print(f"[UnqueuedAction] Deleted unqueued file '{filename}' (Trash bin bypass: {success})")
    return success
