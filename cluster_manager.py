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
            temp_path = target + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(beacon_data, f, indent=2)
            try:
                os.replace(temp_path, target)
            except OSError:
                shutil.copyfile(temp_path, target)
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        except Exception as e:
            print(f"[ClusterManager] Warning publishing master beacon to {target}: {e}")

def discover_workers_from_drive():
    """
    Scans shared Google Drive folder for worker beacon files (data/cluster/workers/worker_*.json).
    Allows Main to discover active helper nodes natively through shared storage.
    """
    search_dirs = [
        getattr(config, "CLUSTER_WORKERS_DIR", os.path.join(config.DATA_DIR, "cluster", "workers")),
        getattr(config, "LOCAL_CLUSTER_WORKERS_DIR", os.path.join(config.LOCAL_DATA_DIR, "cluster", "workers"))
    ]
    seen_ids = set()
    for sdir in search_dirs:
        if not os.path.exists(sdir):
            continue
        try:
            pattern = os.path.join(sdir, "worker_*.json")
            for fpath in glob.glob(pattern):
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        w_data = json.load(f)
                    if isinstance(w_data, dict) and w_data.get("worker_id"):
                        wid = w_data["worker_id"]
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
        w_file = os.path.join(w_dir, f"worker_{worker_id}.json")
        with open(w_file, "w", encoding="utf-8") as f:
            json.dump(worker_data, f, indent=2)
    except Exception:
        pass

    return {
        "status": "registered",
        "worker_id": worker_id,
        "assigned_name": node_name
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
                if chunk.get("worker_id") == worker_id:
                    # Extend chunk lease
                    lease_sec = getattr(config, "CHUNK_LEASE_TIMEOUT_SEC", 90.0)
                    chunk["leased_until"] = time.time() + lease_sec
                    chunk["pct"] = float(metrics.get("pct", chunk.get("pct", 0.0)))
                    chunk["fps"] = float(metrics.get("fps", 0.0))
                    chunk["speed"] = str(metrics.get("speed", "--"))

        database.heartbeat_cluster_worker(worker_id, metrics)

    return {"status": "ok", "extend_lease": True}

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

def claim_task(worker_id: str, node_name: Optional[str] = None, target_job_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Leases the next available video chunk to an idle GPU worker node.
    Prioritizes pending chunks, or expired leased chunks needing recovery.
    If target_job_id is provided, only leases chunks for that specific job.
    """
    with _cluster_lock:
        now = time.time()
        lease_sec = getattr(config, "CHUNK_LEASE_TIMEOUT_SEC", 300.0)

        for job_id, job in _active_cluster_jobs.items():
            if target_job_id is not None and job_id != target_job_id:
                continue
            if job.get("cancelled"):
                continue

            chunks = job.get("chunks", {})
            for c_idx in sorted(chunks.keys()):
                c = chunks[c_idx]
                is_pending = (c["status"] == "pending")
                is_expired = (c["status"] == "leased" and now > c.get("leased_until", 0.0))

                if is_pending or is_expired:
                    # Claim / lease chunk
                    c["status"] = "leased"
                    c["worker_id"] = worker_id
                    c["worker_name"] = node_name or worker_id
                    c["leased_until"] = now + lease_sec

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
                        "total_chunks": len(chunks),
                        "title": job.get("title", f"Job #{job_id}"),
                        "raw_chunk_path": c["raw_path"],
                        "out_chunk_path": c["out_path"],
                        "dur_sec": float(c.get("dur_sec", 240.0)),
                        "refining_config": job.get("refining_config", {}),
                        "lease_seconds": int(lease_sec)
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
        chunk["status"] = "done"
        chunk["pct"] = 100.0
        chunk["file_size_mb"] = file_size_mb
        chunk["worker_id"] = worker_id

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

def reclaim_stale_chunks():
    """Scans all active cluster jobs and automatically returns abandoned chunks to 'pending'."""
    with _cluster_lock:
        now = time.time()
        for job_id, job in _active_cluster_jobs.items():
            if job.get("cancelled"):
                continue
            for c_idx, c in job.get("chunks", {}).items():
                if c.get("status") == "leased" and now > c.get("leased_until", 0.0):
                    abandoned_worker = c.get("worker_id", "unknown")
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

def get_online_helper_count() -> int:
    """Returns the number of active, online remote GPU workers (excluding local node)."""
    discover_workers_from_drive()
    workers = database.get_cluster_workers(active_only=True)
    return sum(1 for w in workers if w.get("status") != "offline" and not str(w.get("worker_id", "")).startswith("main_local"))

def get_cluster_status() -> Dict[str, Any]:
    """Returns a high-level summary of the GPU cluster health, nodes, and jobs."""
    discover_workers_from_drive()
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

    main_node = {
        "worker_id": "main_coordinator",
        "node_name": "Main (Tesla T4)" if has_gpu else "Main (CPU Coordinator)",
        "gpu_name": "NVIDIA GPU NVENC (1 Slot)" if has_gpu else "None (CPU Coordinator)",
        "vram_mb": 15360 if has_gpu else 0,
        "status": main_st,
        "last_heartbeat": time.time(),
        "total_chunks_processed": 0,
        "current_job_id": main_job_id,
        "current_chunk_idx": main_chunk_idx,
        "current_progress": main_pct,
        "current_fps": main_fps,
        "current_speed": main_speed
    }

    all_workers = [main_node] + [w for w in workers if w.get("worker_id") != "main_coordinator"]
    online_count = sum(1 for w in all_workers if w.get("status") != "offline")
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
        "online_nodes": online_count,
        "encoding_nodes": encoding_count,
        "active_jobs": active_jobs_count,
        "total_chunks": total_chunks,
        "done_chunks": done_chunks,
        "workers": all_workers
    }

def _cluster_daemon_loop():
    """Periodic maintenance loop: discovers drive beacons, reclaims dead leases, cleans old nodes."""
    while _daemon_running:
        try:
            discover_workers_from_drive()
            reclaim_stale_chunks()
            database.cleanup_stale_cluster_workers(timeout_sec=120.0)
        except Exception as e:
            print(f"[ClusterDaemon] Exception in cluster loop: {e}")
        time.sleep(5.0)
