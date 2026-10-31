import os
import glob
import json
import time
import shutil
import threading
from typing import Dict, Any, List, Optional, Tuple

import config
import database
import logger

_cluster_lock = threading.RLock()
_active_cluster_jobs: Dict[int, Dict[str, Any]] = {}
_daemon_thread: Optional[threading.Thread] = None
_daemon_running: bool = False
_last_master_url: Optional[str] = None

def start_cluster_daemon():
    """Starts the background cluster supervisor and Drive discovery daemon."""
    global _daemon_thread, _daemon_running
    with _cluster_lock:
        if _daemon_running and _daemon_thread and _daemon_thread.is_alive():
            return
        _daemon_running = True
        _daemon_thread = threading.Thread(target=_cluster_daemon_loop, daemon=True)
        _daemon_thread.start()

def stop_cluster_daemon():
    """Stops the cluster supervisor daemon."""
    global _daemon_running
    with _cluster_lock:
        _daemon_running = False

def publish_master_beacon(master_url: str):
    """
    Publishes Main coordinator's public URL into shared Google Drive.
    Helper notebooks read this beacon file to auto-discover the Main server.
    """
    global _last_master_url
    if master_url and master_url.startswith("http"):
        _last_master_url = master_url.rstrip("/")

    beacon_data = {
        "master_url": master_url,
        "updated_at": time.time(),
        "status": "active"
    }
    targets = [
        getattr(config, "CLUSTER_MASTER_FILE", os.path.join(config.DATA_DIR, "cluster", "master.json")),
        getattr(config, "LOCAL_CLUSTER_MASTER_FILE", os.path.join(config.LOCAL_DATA_DIR, "cluster", "master.json"))
    ]
    for target in targets:
        try:
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "w", encoding="utf-8") as f:
                json.dump(beacon_data, f, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
        except Exception as e:
            print(f"[ClusterManager] Warning publishing master beacon to {target}: {e}")

def reset_cluster_on_startup():
    """
    Cleans up all stale remote workers, lingering worker beacon files, and previous session
    state on coordinator startup so the new session starts with a clean slate.
    """
    clean_inactive_workers(force_all_offline=True)
    logger.notify("CLUSTER", "🧹 GPU Cluster state reset for fresh session: purged stale remote workers.")

def clean_inactive_workers(force_all_offline: bool = False, timeout_sec: float = 15.0) -> int:
    """
    Scans worker beacon files on Google Drive and SQLite database, purging all
    offline, stale, or abandoned worker nodes and their files.
    """
    now = time.time()
    search_dirs = [
        getattr(config, "CLUSTER_WORKERS_DIR", os.path.join(config.DATA_DIR, "cluster", "workers")),
        getattr(config, "LOCAL_CLUSTER_WORKERS_DIR", os.path.join(config.LOCAL_DATA_DIR, "cluster", "workers")),
        os.path.abspath(os.path.join("data", "cluster", "workers")),
        os.path.abspath(os.path.join("..", "data", "cluster", "workers"))
    ]
    if getattr(config, "IS_COLAB", False):
        mydrive = "/content/drive/MyDrive"
        if os.path.exists(mydrive):
            try:
                for entry in os.listdir(mydrive):
                    cand = os.path.join(mydrive, entry, "data", "cluster", "workers")
                    if os.path.isdir(cand) and cand not in search_dirs:
                        search_dirs.append(cand)
            except Exception:
                pass

    seen_dirs = set()
    for sdir in search_dirs:
        norm = os.path.abspath(sdir)
        if norm in seen_dirs or not os.path.exists(norm):
            continue
        seen_dirs.add(norm)
        try:
            try:
                os.listdir(norm)  # Busts Drive FUSE cache
            except Exception:
                pass
            for fpath in glob.glob(os.path.join(norm, "worker_*.json")):
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        w = json.load(f)
                    hb = float(w.get("last_heartbeat") or 0.0)
                    st = str(w.get("status", "")).lower()
                    wid = w.get("worker_id", "")
                    if wid and not force_all_offline:
                        db_w = database.get_cluster_worker(wid)
                        if db_w:
                            db_hb = float(db_w.get("last_heartbeat") or 0.0)
                            if db_hb > hb:
                                hb = db_hb
                            db_st = str(db_w.get("status", "")).lower()
                            if db_st:
                                st = db_st
                    effective_timeout = max(60.0, timeout_sec) if st == "encoding" else max(30.0, timeout_sec)
                    if force_all_offline or st in ["offline", "waiting_for_master"] or (now - hb) > effective_timeout:
                        try:
                            config.bypass_trash_delete(fpath)
                        except Exception:
                            try:
                                os.remove(fpath)
                            except Exception:
                                pass
                        if wid:
                            database.remove_cluster_worker(wid)
                except Exception:
                    # Do not delete valid files on transient FUSE read/lock errors
                    continue
        except Exception:
            pass

    if force_all_offline:
        return database.purge_all_remote_workers()
    else:
        return database.cleanup_stale_cluster_workers(timeout_sec=timeout_sec)

def discover_workers_from_drive():
    """
    Scans shared Google Drive folder for worker beacon files (data/cluster/workers/worker_*.json).
    Allows Main to discover active helper nodes natively through shared storage.
    Prunes stale worker files whose last heartbeat is older than 15 seconds.
    """
    search_dirs = [
        getattr(config, "CLUSTER_WORKERS_DIR", os.path.join(config.DATA_DIR, "cluster", "workers")),
        getattr(config, "LOCAL_CLUSTER_WORKERS_DIR", os.path.join(config.LOCAL_DATA_DIR, "cluster", "workers")),
        os.path.abspath(os.path.join("data", "cluster", "workers")),
        os.path.abspath(os.path.join("..", "data", "cluster", "workers"))
    ]
    if getattr(config, "IS_COLAB", False):
        mydrive = "/content/drive/MyDrive"
        if os.path.exists(mydrive):
            try:
                for entry in os.listdir(mydrive):
                    cand = os.path.join(mydrive, entry, "data", "cluster", "workers")
                    if os.path.isdir(cand) and cand not in search_dirs:
                        search_dirs.append(cand)
            except Exception:
                pass

    seen_ids = set()
    now = time.time()
    for sdir in search_dirs:
        if not os.path.exists(sdir):
            continue
        try:
            try:
                os.listdir(sdir)  # Invalidate Google Drive FUSE cache
            except Exception:
                pass
            pattern = os.path.join(sdir, "worker_*.json")
            for fpath in glob.glob(pattern):
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        w_data = json.load(f)
                    if isinstance(w_data, dict) and w_data.get("worker_id"):
                        wid = w_data["worker_id"]
                        hb = float(w_data.get("last_heartbeat") or 0.0)
                        st = str(w_data.get("status", "")).lower()

                        # Protect actively connected HTTP workers
                        db_w = database.get_cluster_worker(wid)
                        if db_w:
                            db_hb = float(db_w.get("last_heartbeat") or 0.0)
                            if db_hb > hb:
                                hb = db_hb
                            db_st = str(db_w.get("status", "")).lower()
                            if db_st:
                                st = db_st

                        effective_timeout = max(60.0, 30.0) if st == "encoding" else 30.0
                        if (now - hb) > effective_timeout or st in ["offline", "waiting_for_master"]:
                            # Prune stale dead worker beacon from Drive so it doesn't linger
                            try:
                                config.bypass_trash_delete(fpath)
                            except Exception:
                                try:
                                    os.remove(fpath)
                                except Exception:
                                    pass
                            database.remove_cluster_worker(wid)
                            continue

                        if wid not in seen_ids:
                            seen_ids.add(wid)
                            database.upsert_cluster_worker(w_data)
                except Exception:
                    continue
        except Exception:
            pass

def register_worker(worker_data: Dict[str, Any]) -> Dict[str, Any]:
    """Registers a remote GPU worker via direct HTTP API call."""
    worker_id = worker_data.get("worker_id")
    if not worker_id:
        return {"status": "error", "message": "Missing worker_id"}

    gpu_name = worker_data.get("gpu_name", "GPU")
    node_name = worker_data.get("node_name", f"Worker ({gpu_name})")
    worker_data["node_name"] = node_name
    database.upsert_cluster_worker(worker_data)
    logger.success("CLUSTER", f"🟢 Remote GPU Worker registered: {node_name} [{worker_id}] ({gpu_name}, {worker_data.get('vram_mb', 0)} MB VRAM)")

    # Also mirror worker file to local cluster workers directory
    try:
        w_dir = getattr(config, "CLUSTER_WORKERS_DIR", os.path.join(config.DATA_DIR, "cluster", "workers"))
        os.makedirs(w_dir, exist_ok=True)
        b_fname = f"{worker_id}.json" if worker_id.startswith("worker_") else f"worker_{worker_id}.json"
        w_file = os.path.join(w_dir, b_fname)
        with open(w_file, "w", encoding="utf-8") as f:
            json.dump(worker_data, f, indent=2)
    except Exception:
        pass

    downloader_dir = getattr(config, "DOWNLOADER_DIR", "")
    folder_name = os.path.basename(downloader_dir) if downloader_dir else ""

    return {
        "status": "registered",
        "worker_id": worker_id,
        "assigned_name": node_name,
        "downloader_dir": downloader_dir,
        "folder_name": folder_name
    }

def heartbeat_worker(worker_id: str, metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Processes periodic worker heartbeat and updates active chunk lease."""
    if not worker_id:
        return {"status": "error", "message": "Missing worker_id"}

    metrics = metrics or {}
    job_id = metrics.get("job_id")
    chunk_idx = metrics.get("chunk_idx")

    with _cluster_lock:
        if job_id in _active_cluster_jobs and chunk_idx is not None:
            c_idx = int(chunk_idx)
            job = _active_cluster_jobs[job_id]
            if c_idx in job["chunks"]:
                chunk = job["chunks"][c_idx]

                # Check if chunk was already completed by another worker (e.g. winner of speculative race)
                if chunk.get("status") == "done" and chunk.get("worker_id") != worker_id:
                    return {"status": "ok", "extend_lease": False, "abort_chunk": True}

                is_primary = (chunk.get("worker_id") == worker_id)
                is_speculative = (chunk.get("speculative_worker_id") == worker_id)

                if is_primary or is_speculative:
                    now = time.time()
                    lease_sec = getattr(config, "CHUNK_LEASE_TIMEOUT_SEC", 90.0)

                    new_pct = float(metrics.get("pct", chunk.get("pct", 0.0)))
                    # Track forward progress
                    if new_pct > float(chunk.get("last_progress_pct", 0.0)):
                        chunk["last_progress_pct"] = new_pct
                        chunk["last_progress_time"] = now

                    chunk["pct"] = new_pct
                    chunk["fps"] = float(metrics.get("fps", 0.0))
                    chunk["speed"] = str(metrics.get("speed", "--"))

                    # If stuck at 0.0% progress for > 90s, do not allow infinite renewal of primary lease
                    l_start = float(chunk.get("lease_start") or now)
                    if new_pct == 0.0 and (now - l_start) > 90.0:
                        chunk["leased_until"] = min(chunk.get("leased_until", now), l_start + 90.0)
                    else:
                        if is_primary:
                            chunk["leased_until"] = now + lease_sec
                        else:
                            chunk["speculative_until"] = now + lease_sec

        database.heartbeat_cluster_worker(worker_id, metrics)

    return {"status": "ok", "extend_lease": True, "abort_chunk": False}

def register_cluster_job(job_id: int, job_info: Dict[str, Any]):
    """Registers a multi-chunk video refining job with the cluster manager."""
    with _cluster_lock:
        _active_cluster_jobs[job_id] = job_info

def unregister_cluster_job(job_id: int):
    """Unregisters a completed or cancelled cluster refining job."""
    with _cluster_lock:
        _active_cluster_jobs.pop(job_id, None)

def get_cluster_job(job_id: int) -> Optional[Dict[str, Any]]:
    """Retrieves cluster job state."""
    with _cluster_lock:
        return _active_cluster_jobs.get(job_id)

def claim_task(worker_id: str, node_name: Optional[str] = None, target_job_id: Optional[int] = None, exclude_chunks: Optional[List[Any]] = None) -> Dict[str, Any]:
    """
    Leases the next available video chunk to an idle GPU worker node.
    Prioritizes pending chunks, or expired leased chunks needing recovery.
    If all chunks are leased, checks for stuck/slow chunks eligible for speculative execution.
    If target_job_id is provided, only leases chunks for that specific job.
    """
    with _cluster_lock:
        now = time.time()
        lease_sec = getattr(config, "CHUNK_LEASE_TIMEOUT_SEC", 300.0)

        exclude_set = set()
        if exclude_chunks:
            for x in exclude_chunks:
                if isinstance(x, (tuple, list)) and len(x) == 2:
                    exclude_set.add(f"{x[0]}:{x[1]}")
                else:
                    exclude_set.add(str(x))

        # -------------------------------------------------------------
        # PASS 1: Pending chunks or expired primary leases across all jobs
        # -------------------------------------------------------------
        for job_id, job in _active_cluster_jobs.items():
            if target_job_id is not None and job_id != target_job_id:
                continue
            if job.get("cancelled"):
                continue

            chunks = job.get("chunks", {})
            unfinished_count = sum(1 for c in chunks.values() if c.get("status") != "done")
            # Dynamic lease: for tail-end chunks (<= 2 left), drop lease from 300s to 60s
            effective_lease_sec = 60.0 if unfinished_count <= 2 else lease_sec

            for c_idx in sorted(chunks.keys()):
                if exclude_set and f"{job_id}:{c_idx}" in exclude_set:
                    continue
                c = chunks[c_idx]
                is_pending = (c["status"] == "pending")
                is_expired = (c["status"] == "leased" and now > c.get("leased_until", 0.0))

                if is_pending or is_expired:
                    # Claim / lease chunk
                    c["status"] = "leased"
                    c["worker_id"] = worker_id
                    c["worker_name"] = node_name or worker_id
                    c["leased_until"] = now + effective_lease_sec
                    c["lease_start"] = now
                    c["last_progress_pct"] = c.get("pct", 0.0)
                    c["last_progress_time"] = now
                    c["speculative_worker_id"] = None
                    c["speculative_until"] = 0.0

                    # Update database worker tracking
                    database.heartbeat_cluster_worker(worker_id, {
                        "job_id": job_id,
                        "chunk_idx": c_idx,
                        "pct": c.get("pct", 0.0),
                        "fps": 0.0,
                        "speed": "--",
                        "status": "encoding"
                    })

                    worker_label = node_name or worker_id
                    logger.notify("REFINER", f"⚡ Starting transcode of Chunk #{c_idx + 1}/{len(chunks)} for Job #{job_id} ('{job.get('title', f'Job #{job_id}')}') on '{worker_label}'")

                    return {
                        "has_task": True,
                        "job_id": job_id,
                        "chunk_idx": c_idx,
                        "is_speculative": False,
                        "total_chunks": len(chunks),
                        "title": job.get("title", f"Job #{job_id}"),
                        "raw_chunk_path": c["raw_path"],
                        "out_chunk_path": c["out_path"],
                        "dur_sec": float(c.get("dur_sec", 240.0)),
                        "refining_config": job.get("refining_config", {}),
                        "lease_seconds": int(effective_lease_sec)
                    }

        # -------------------------------------------------------------
        # PASS 2: Speculative Execution for Stragglers / Stuck Chunks
        # If all chunks are leased and this worker is idle, duplicate a slow/stalled chunk!
        # -------------------------------------------------------------
        speculative_timeout = getattr(config, "SPECULATIVE_CHUNK_STALL_SEC", 45.0)
        for job_id, job in _active_cluster_jobs.items():
            if target_job_id is not None and job_id != target_job_id:
                continue
            if job.get("cancelled"):
                continue

            chunks = job.get("chunks", {})
            for c_idx in sorted(chunks.keys()):
                if exclude_set and f"{job_id}:{c_idx}" in exclude_set:
                    continue
                c = chunks[c_idx]
                if c.get("status") != "leased":
                    continue
                # Do not duplicate to the same worker that already holds the primary lease
                if c.get("worker_id") == worker_id:
                    continue
                # Do not double-speculate if another worker is already speculatively computing it
                if c.get("speculative_worker_id") is not None and now <= c.get("speculative_until", 0.0):
                    continue

                lease_start = float(c.get("lease_start") or now)
                last_prog_time = float(c.get("last_progress_time") or lease_start)
                lease_duration = now - lease_start
                time_since_progress = now - last_prog_time
                dur_sec = float(c.get("dur_sec", 240.0))

                is_stalled = (lease_duration >= speculative_timeout and time_since_progress >= speculative_timeout)
                is_abnormally_slow = (lease_duration >= (dur_sec * 1.5) and float(c.get("pct", 0.0)) < 70.0)

                if is_stalled or is_abnormally_slow:
                    c["speculative_worker_id"] = worker_id
                    c["speculative_worker_name"] = node_name or worker_id
                    c["speculative_until"] = now + 90.0

                    database.heartbeat_cluster_worker(worker_id, {
                        "job_id": job_id,
                        "chunk_idx": c_idx,
                        "pct": 0.0,
                        "fps": 0.0,
                        "speed": "--",
                        "status": "encoding"
                    })

                    worker_label = node_name or worker_id
                    orig_name = c.get("worker_name", c.get("worker_id", "unknown"))
                    curr_pct = float(c.get("pct", 0.0))
                    logger.notify("REFINER", f"⚡ [Speculative] Duplicating slow Chunk #{c_idx + 1} of Job #{job_id} onto idle helper '{worker_label}' (Original '{orig_name}' stalled for {int(time_since_progress)}s at {curr_pct:.1f}%)...")

                    return {
                        "has_task": True,
                        "job_id": job_id,
                        "chunk_idx": c_idx,
                        "is_speculative": True,
                        "total_chunks": len(chunks),
                        "title": job.get("title", f"Job #{job_id}"),
                        "raw_chunk_path": c["raw_path"],
                        "out_chunk_path": c["out_path"],
                        "dur_sec": dur_sec,
                        "refining_config": job.get("refining_config", {}),
                        "lease_seconds": 90
                    }

    return {"has_task": False, "wait_seconds": 5}

def complete_task(worker_id: str, job_id: int, chunk_idx: int, out_chunk_path: str, file_size_mb: float = 0.0) -> Dict[str, Any]:
    """Marks a chunk successfully converted by a worker and checks if job is ready to mux."""
    with _cluster_lock:
        if job_id not in _active_cluster_jobs:
            return {"status": "error", "message": f"Job #{job_id} not active in cluster"}

        job = _active_cluster_jobs[job_id]
        chunks = job.get("chunks", {})
        if chunk_idx not in chunks:
            return {"status": "error", "message": f"Chunk #{chunk_idx} not found in job #{job_id}"}

        chunk = chunks[chunk_idx]
        if chunk.get("status") == "done":
            # Another worker already finished this chunk (first to finish won)
            return {
                "status": "already_done",
                "chunk_idx": chunk_idx,
                "winner": chunk.get("worker_id"),
                "all_chunks_done": all(c.get("status") == "done" for c in chunks.values())
            }

        chunk["status"] = "done"
        chunk["pct"] = 100.0
        chunk["file_size_mb"] = file_size_mb
        chunk["worker_id"] = worker_id
        chunk["speculative_worker_id"] = None
        chunk["speculative_until"] = 0.0

        database.increment_worker_chunks(worker_id)

        all_done = all(c.get("status") == "done" for c in chunks.values())
        done_count = sum(1 for c in chunks.values() if c.get("status") == "done")
        pct = (done_count / len(chunks)) * 100.0 if chunks else 100.0
        logger.success("REFINER", f"✅ Chunk #{chunk_idx + 1}/{len(chunks)} of Job #{job_id} finished by '{worker_id}' ({file_size_mb:.1f} MB). Overall progress: {done_count}/{len(chunks)} chunks done ({pct:.1f}%).")
        return {
            "status": "accepted",
            "chunk_idx": chunk_idx,
            "all_chunks_done": all_done
        }

def release_task(worker_id: str, job_id: int, chunk_idx: int) -> Dict[str, Any]:
    """Releases a claimed/leased chunk back to pending status so another worker or retry can process it."""
    with _cluster_lock:
        if job_id not in _active_cluster_jobs:
            return {"status": "error", "message": f"Job #{job_id} not active"}
        job = _active_cluster_jobs[job_id]
        chunks = job.get("chunks", {})
        if chunk_idx not in chunks:
            return {"status": "error", "message": f"Chunk #{chunk_idx} not found in job #{job_id}"}
        chunk = chunks[chunk_idx]

        if chunk.get("speculative_worker_id") == worker_id:
            chunk["speculative_worker_id"] = None
            chunk["speculative_until"] = 0.0
            return {"status": "speculative_released", "job_id": job_id, "chunk_idx": chunk_idx}

        if chunk.get("status") == "leased" and (chunk.get("worker_id") == worker_id or not worker_id):
            if chunk.get("speculative_worker_id"):
                # Promote speculative worker to primary
                chunk["worker_id"] = chunk["speculative_worker_id"]
                chunk["worker_name"] = chunk.get("speculative_worker_name")
                chunk["leased_until"] = chunk.get("speculative_until", 0.0)
                chunk["speculative_worker_id"] = None
                chunk["speculative_until"] = 0.0
                logger.warn("REFINER", f"↩️ Chunk #{chunk_idx + 1} of Job #{job_id} primary worker released; promoted speculative worker '{chunk['worker_id']}'.")
            else:
                chunk["status"] = "pending"
                chunk["worker_id"] = None
                chunk["leased_until"] = 0.0
                chunk["pct"] = 0.0
                logger.warn("REFINER", f"↩️ Chunk #{chunk_idx + 1} of Job #{job_id} released back to pending by '{worker_id}'.")
            return {"status": "released", "job_id": job_id, "chunk_idx": chunk_idx}
        return {"status": "ignored", "current_status": chunk.get("status")}

def get_chunk_raw_path(job_id: int, chunk_idx: int) -> Optional[str]:
    """Returns the local/drive raw chunk path on the coordinator for direct HTTP streaming."""
    with _cluster_lock:
        job = _active_cluster_jobs.get(job_id)
        if not job:
            return None
        chunk = job.get("chunks", {}).get(chunk_idx)
        if not chunk:
            return None
        return chunk.get("raw_path")

def reclaim_stale_chunks():
    """Scans all active cluster jobs and automatically returns abandoned chunks to 'pending'."""
    with _cluster_lock:
        now = time.time()
        for job_id, job in _active_cluster_jobs.items():
            if job.get("cancelled"):
                continue
            for c_idx, c in job.get("chunks", {}).items():
                # Reclaim expired speculative lease
                if c.get("speculative_worker_id") and now > c.get("speculative_until", 0.0):
                    c["speculative_worker_id"] = None
                    c["speculative_until"] = 0.0

                if c.get("status") == "leased" and now > c.get("leased_until", 0.0):
                    abandoned_worker = c.get("worker_id", "unknown")
                    # If there's an active speculative worker, promote it instead of resetting to pending
                    if c.get("speculative_worker_id") and now <= c.get("speculative_until", 0.0):
                        c["worker_id"] = c["speculative_worker_id"]
                        c["worker_name"] = c.get("speculative_worker_name")
                        c["leased_until"] = c.get("speculative_until", 0.0)
                        c["speculative_worker_id"] = None
                        c["speculative_until"] = 0.0
                        logger.warn("REFINER", f"⚠️ Primary lease expired for Chunk #{c_idx + 1} of Job #{job_id} from '{abandoned_worker}'; promoted speculative worker '{c['worker_id']}'.")
                    else:
                        logger.warn("REFINER", f"⚠️ Chunk #{c_idx + 1} of Job #{job_id} lease expired from worker '{abandoned_worker}'! Reclaimed back to pending for retry.")
                        c["status"] = "pending"
                        c["worker_id"] = None
                        c["leased_until"] = 0.0
                        c["pct"] = 0.0

                    # Clean up partial output file if present
                    out_part = c.get("out_path", "") + ".part"
                    if os.path.exists(out_part):
                        try:
                            os.remove(out_part)
                        except Exception:
                            pass

def get_active_cluster_jobs_count() -> int:
    """Returns the number of video jobs currently active in the cluster pool."""
    with _cluster_lock:
        return len(_active_cluster_jobs)

def get_total_cluster_slots() -> int:
    """Returns the total number of concurrent transcoding slots available across the cluster."""
    status = get_cluster_status()
    return max(1, int(status.get("total_slots", 1)))

def get_pending_chunk_count() -> int:
    """Returns the total number of unassigned or expired chunks across all active jobs."""
    with _cluster_lock:
        now = time.time()
        count = 0
        for job in _active_cluster_jobs.values():
            if job.get("cancelled"):
                continue
            for c in job.get("chunks", {}).values():
                if c.get("status") == "pending" or (c.get("status") == "leased" and now > c.get("leased_until", 0.0)):
                    count += 1
        return count

def should_prechunk_next_video() -> bool:
    """
    Lookahead check: Returns True if the number of pending chunks in the cluster
    is less than the total available GPU transcoding slots, meaning upcoming slots will soon idle.
    """
    pending = get_pending_chunk_count()
    total_slots = get_total_cluster_slots()
    return pending < total_slots

def get_online_helper_count() -> int:
    """Returns the number of active, online remote GPU workers (excluding local node)."""
    discover_workers_from_drive()
    workers = database.get_cluster_workers(active_only=True)
    return sum(1 for w in workers if w.get("status") != "offline" and not str(w.get("worker_id", "")).startswith("main_local"))

def get_cluster_status() -> Dict[str, Any]:
    """Returns a high-level summary of the GPU cluster health, nodes, and jobs."""
    discover_workers_from_drive()
    clean_inactive_workers(force_all_offline=False, timeout_sec=15.0)
    workers = database.get_cluster_workers(active_only=False)

    has_gpu = False
    try:
        import encoder
        has_gpu = encoder.is_nvenc_available()
    except Exception:
        pass

    # Check if Main GPU is actively encoding
    main_st = "idle"
    main_pct = 0.0
    main_fps = 0.0
    main_speed = "--"
    main_job_id = 0
    main_chunk_idx = -1
    with _cluster_lock:
        for j_id, j in _active_cluster_jobs.items():
            for c_i, c in j.get("chunks", {}).items():
                if c.get("status") == "leased" and "main" in str(c.get("worker_id", "")).lower():
                    main_st = "encoding"
                    main_job_id = j_id
                    main_chunk_idx = c_i
                    main_pct = c.get("pct", 0.0)
                    main_fps = c.get("fps", 0.0)
                    main_speed = c.get("speed", "--")
                    break
            if main_st == "encoding":
                break

    main_db = next((w for w in workers if w.get("worker_id") == "main_coordinator"), {})
    main_chunks_processed = int(main_db.get("total_chunks_processed", 0))

    main_node = {
        "worker_id": "main_coordinator",
        "node_name": "Main (Tesla T4)" if has_gpu else "Main (CPU Coordinator)",
        "gpu_name": "NVIDIA GPU NVENC (1 Slot)" if has_gpu else "None (CPU Coordinator)",
        "vram_mb": 15360 if has_gpu else 0,
        "status": main_st if has_gpu else "coordinator",
        "last_heartbeat": time.time(),
        "total_chunks_processed": main_chunks_processed,
        "current_job_id": main_job_id,
        "current_chunk_idx": main_chunk_idx,
        "current_progress": main_pct,
        "current_fps": main_fps,
        "current_speed": main_speed
    }

    # Filter out workers that have no heartbeat for > 15s (stale/disconnected) or are not ready
    now = time.time()
    active_remote = [
        w for w in workers 
        if w.get("worker_id") != "main_coordinator" 
        and w.get("status") in ["idle", "encoding"]
        and (now - float(w.get("last_heartbeat") or 0.0)) <= 15.0
    ]

    # For display in dashboard, show any active remote worker seen in last 15s
    visible_remote = [
        w for w in workers
        if w.get("worker_id") != "main_coordinator"
        and w.get("status") not in ["offline", "waiting_for_master"]
        and (now - float(w.get("last_heartbeat") or 0.0)) <= 15.0
    ]

    # Only include main_node in GPU compute worker cards if Main actually has a hardware GPU
    if has_gpu:
        all_workers = [main_node] + visible_remote
        online_gpus = len(active_remote) + 1
        total_slots = 1 + (len(active_remote) * 2)
    else:
        all_workers = visible_remote
        online_gpus = len(active_remote)
        total_slots = len(active_remote) * 2

    online_count = sum(1 for w in all_workers if w.get("status") not in ["offline", "coordinator", "waiting_for_master"])
    encoding_count = sum(1 for w in all_workers if w.get("status") == "encoding")

    with _cluster_lock:
        total_chunks = 0
        done_chunks = 0
        active_jobs_count = len(_active_cluster_jobs)
        for job in _active_cluster_jobs.values():
            chunks = job.get("chunks", {})
            total_chunks += len(chunks)
            done_chunks += sum(1 for c in chunks.values() if c.get("status") == "done")

    return {
        "total_nodes": len(all_workers),
        "total_gpus": online_gpus,
        "total_slots": total_slots,
        "online_nodes": online_count,
        "encoding_nodes": encoding_count,
        "active_jobs": active_jobs_count,
        "total_chunks": total_chunks,
        "done_chunks": done_chunks,
        "main_has_gpu": has_gpu,
        "coordinator_node": main_node,
        "workers": all_workers
    }

def _cluster_daemon_loop():
    """Periodic maintenance loop: re-publishes master beacon, discovers drive beacons, reclaims dead leases, cleans old nodes."""
    last_beacon_refresh = 0.0
    while _daemon_running:
        try:
            now = time.time()
            if _last_master_url and (now - last_beacon_refresh) >= 5.0:
                publish_master_beacon(_last_master_url)
                last_beacon_refresh = now

            discover_workers_from_drive()
            reclaim_stale_chunks()
            clean_inactive_workers(force_all_offline=False, timeout_sec=15.0)
        except Exception as e:
            print(f"[ClusterDaemon] Exception in cluster loop: {e}")
        time.sleep(3.0)
