import os
import sys
import time
import json
import uuid
import shutil
import urllib.request
import urllib.error
import urllib.parse
import subprocess
import threading
from typing import Dict, Any, Optional, List

import config
import logger

def get_gpu_info() -> Dict[str, Any]:
    """Inspects system hardware for NVIDIA GPU specs."""
    info = {
        "gpu_name": "CPU Worker",
        "vram_mb": 0,
        "driver": ""
    }
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name,memory.total,driver_version", "--format=csv,noheader,nounits"],
            text=True
        ).strip()
        parts = [p.strip() for p in out.split(",")]
        if len(parts) >= 3:
            info["gpu_name"] = parts[0]
            info["vram_mb"] = int(parts[1])
            info["driver"] = parts[2]
    except Exception:
        pass
    return info

def is_nvenc_available() -> bool:
    """Checks if NVENC hevc/h264 encoders are supported on this worker."""
    try:
        out = subprocess.check_output(["ffmpeg", "-encoders"], text=True, stderr=subprocess.DEVNULL)
        return "hevc_nvenc" in out or "h264_nvenc" in out
    except Exception:
        return False

class HelperWorker:
    """
    Autonomous GPU Refiner Worker node running inside a Colab helper notebook.
    Discovers Main via shared Drive, claims up to 2 concurrent video chunks,
    encodes via hardware NVENC, and returns completed segments directly to shared Drive.
    """
    MAX_CONCURRENT_CHUNKS = 2

    def __init__(self, worker_id: Optional[str] = None):
        self.gpu_info = get_gpu_info()
        short_gpu = self.gpu_info["gpu_name"].replace("NVIDIA", "").replace("Tesla", "").strip() or "Node"
        self.worker_id = worker_id or f"worker_{short_gpu.lower()}_{uuid.uuid4().hex[:8]}"
        self.node_name = f"Helper ({self.gpu_info['gpu_name']})"
        self.master_url: Optional[str] = None
        self.running: bool = True
        self.active_slots: Dict[int, Optional[Dict[str, Any]]] = {0: None, 1: None}
        self.active_slots_lock = threading.Lock()
        self.local_temp_dir = os.path.join(getattr(config, "LOCAL_WORKSPACE", "/content/temp"), "helper_work")
        os.makedirs(self.local_temp_dir, exist_ok=True)

    def discover_master_url(self) -> Optional[str]:
        """Reads master beacon file from shared Google Drive."""
        targets = [
            getattr(config, "CLUSTER_MASTER_FILE", os.path.join(config.DATA_DIR, "cluster", "master.json")),
            getattr(config, "LOCAL_CLUSTER_MASTER_FILE", os.path.join(config.LOCAL_DATA_DIR, "cluster", "master.json"))
        ]
        for t in targets:
            if os.path.exists(t):
                try:
                    with open(t, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    url = data.get("master_url")
                    if url and str(url).startswith("http"):
                        return url.rstrip("/")
                except Exception:
                    pass
        return None

    def announce_to_drive(self, status: str = "idle"):
        """Writes collision-free worker beacon file directly to shared Google Drive."""
        data = {
            "worker_id": self.worker_id,
            "node_name": self.node_name,
            "gpu_name": self.gpu_info["gpu_name"],
            "vram_mb": self.gpu_info["vram_mb"],
            "status": status,
            "last_heartbeat": time.time(),
            "registered_at": time.time()
        }
        targets = [
            getattr(config, "CLUSTER_WORKERS_DIR", os.path.join(config.DATA_DIR, "cluster", "workers")),
            getattr(config, "LOCAL_CLUSTER_WORKERS_DIR", os.path.join(config.LOCAL_DATA_DIR, "cluster", "workers"))
        ]
        for tdir in targets:
            try:
                os.makedirs(tdir, exist_ok=True)
                w_file = os.path.join(tdir, f"worker_{self.worker_id}.json")
                with open(w_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
            except Exception:
                pass

    def _http_request(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Sends an HTTP POST or GET request with browser headers to avoid Cloudflare bot blocking."""
        if not self.master_url:
            return None
        url = f"{self.master_url}{endpoint}"
        try:
            req_data = None
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "X-Worker-ID": self.worker_id
            }
            if data is not None:
                req_data = json.dumps(data).encode("utf-8")
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(url, data=req_data, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status == 200:
                    body = resp.read().decode("utf-8")
                    return json.loads(body)
        except urllib.error.HTTPError as e:
            if "claim_task" not in endpoint:
                logger.warn("HELPER", f"HTTP {e.code} on {endpoint}: {e.reason}")
            return None
        except urllib.error.URLError as e:
            if "claim_task" not in endpoint and "timed out" not in str(e).lower():
                logger.warn("HELPER", f"Connection issue on {endpoint}: {e.reason}")
            return None
        except TimeoutError:
            return None
        except Exception as e:
            if "timed out" not in str(e).lower():
                logger.debug("HELPER", f"Exception on {endpoint}: {e}")
            return None
        return None

    def register_with_master(self) -> bool:
        """Registers worker directly with Main via HTTP API and Drive Beacon."""
        reg_payload = {
            "worker_id": self.worker_id,
            "node_name": self.node_name,
            "gpu_name": self.gpu_info["gpu_name"],
            "vram_mb": self.gpu_info["vram_mb"]
        }
        logger.notify("HELPER", f"📡 Registering with Main coordinator ({self.master_url})...")
        res = self._http_request("/api/cluster/register", reg_payload)
        if res and res.get("status") == "registered":
            assigned = res.get("assigned_name", self.node_name)
            logger.success("HELPER", f"Successfully registered with Main coordinator as '{assigned}'!")
        else:
            logger.warn("HELPER", "Direct HTTP registration pending; synced via Google Drive shared beacon.")

        self.announce_to_drive(status="idle")
        return True

    def send_heartbeat(self, metrics: Optional[Dict[str, Any]] = None) -> bool:
        """Sends live progress and heartbeat to Main."""
        payload = metrics or {}
        payload["worker_id"] = self.worker_id
        res = self._http_request("/api/cluster/heartbeat", payload)
        self.announce_to_drive(status=payload.get("status", "idle"))
        if payload.get("status") == "encoding":
            logger.debug("HELPER", f"💓 Heartbeat: Chunk #{payload.get('chunk_idx')} ({payload.get('pct', 0.0):.1f}%) @ {payload.get('fps', 0)} fps")
        return bool(res and res.get("status") == "ok")

    def claim_task(self) -> Optional[Dict[str, Any]]:
        """Polls Main for next available chunk assignment."""
        endpoint = f"/api/cluster/claim_task?worker_id={self.worker_id}&node_name={urllib.parse.quote(self.node_name)}"
        res = self._http_request(endpoint)
        if res and res.get("has_task"):
            logger.notify("HELPER", f"📥 Claimed Chunk #{res.get('chunk_idx', 0) + 1}/{res.get('total_chunks')} of Job #{res.get('job_id')} ('{res.get('title', 'Job')}')")
            return res
        return None

    def complete_task(self, job_id: int, chunk_idx: int, out_chunk_path: str, file_size_mb: float = 0.0) -> bool:
        """Submits chunk completion notice to Main."""
        payload = {
            "worker_id": self.worker_id,
            "job_id": job_id,
            "chunk_idx": chunk_idx,
            "out_chunk_path": out_chunk_path,
            "file_size_mb": file_size_mb
        }
        res = self._http_request("/api/cluster/complete_task", payload)
        logger.debug("HELPER", f"📤 Notified Main coordinator of chunk #{chunk_idx} completion (Job #{job_id}).")
        return bool(res and res.get("status") == "accepted")

    def release_task(self, job_id: int, chunk_idx: int) -> bool:
        """Notifies coordinator to release a claimed chunk back to pending status for retry."""
        payload = {
            "worker_id": self.worker_id,
            "job_id": job_id,
            "chunk_idx": chunk_idx
        }
        res = self._http_request("/api/cluster/release_task", payload)
        logger.debug("HELPER", f"↩️ Released Chunk #{chunk_idx + 1} of Job #{job_id} back to pending.")
        return bool(res and res.get("status") == "released")

    def download_chunk_file(self, endpoint: str, target_local_path: str) -> bool:
        """Downloads raw chunk file directly from Main coordinator via HTTP stream if Drive sync is pending."""
        if not self.master_url:
            return False
        url = f"{self.master_url.rstrip('/')}{endpoint}"
        req = urllib.request.Request(url, headers={"User-Agent": f"ColabGPUHelper/{self.worker_id}"})
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                if resp.status != 200:
                    return False
                with open(target_local_path, "wb") as out_f:
                    shutil.copyfileobj(resp, out_f)
            return os.path.exists(target_local_path) and os.path.getsize(target_local_path) > 0
        except Exception as e:
            logger.debug("HELPER", f"HTTP chunk stream download error ({url}): {e}")
            return False

    def encode_chunk(self, task: Dict[str, Any], slot_id: int = 0) -> bool:
        """
        Executes hardware-accelerated NVENC transcode on local SSD copy of chunk,
        sending periodic heartbeats, then copies finished .mkv directly to Drive.
        """
        raw_drive_chunk = task["raw_chunk_path"]
        out_drive_chunk = task["out_chunk_path"]
        ref_cfg = task.get("refining_config", {})
        job_id = task["job_id"]
        chunk_idx = task["chunk_idx"]

        chunk_name = os.path.basename(raw_drive_chunk)
        local_raw = os.path.join(self.local_temp_dir, f"in_slot{slot_id}_{chunk_name}")
        local_out = os.path.join(self.local_temp_dir, f"out_slot{slot_id}_{chunk_name}")

        for f in (local_raw, local_out):
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

        # 1. Fetch raw segment: Try Google Drive first (with retry/cache invalidation), fallback to direct HTTP stream
        found_on_drive = False
        for attempt in range(4):
            if os.path.exists(raw_drive_chunk) and os.path.getsize(raw_drive_chunk) > 0:
                found_on_drive = True
                break
            # Force Colab FUSE directory cache refresh by re-listing parent directory
            try:
                p_dir = os.path.dirname(raw_drive_chunk)
                if os.path.exists(p_dir):
                    os.listdir(p_dir)
            except Exception:
                pass
            time.sleep(1.0)

        if found_on_drive:
            try:
                shutil.copyfile(raw_drive_chunk, local_raw)
            except Exception as e:
                logger.warn("HELPER", f"[Slot {slot_id}] Drive copy failed ({e}); falling back to HTTP stream...")
                found_on_drive = False

        if not found_on_drive or not os.path.exists(local_raw) or os.path.getsize(local_raw) == 0:
            logger.notify("HELPER", f"⏳ Chunk #{chunk_idx + 1} pending Drive sync. Streaming directly from Main coordinator via HTTP...")
            chunk_ep = f"/api/cluster/chunk/{job_id}/{chunk_idx}"
            dl_ok = self.download_chunk_file(chunk_ep, local_raw)
            if not dl_ok:
                logger.warn("HELPER", f"[Slot {slot_id}] ⚠️ Source chunk #{chunk_idx + 1} not yet accessible on Drive or HTTP. Releasing back to queue...")
                self.release_task(job_id, chunk_idx)
                return False
            f_mb = os.path.getsize(local_raw) / (1024 * 1024)
            logger.notify("HELPER", f"✅ Chunk #{chunk_idx + 1} ({f_mb:.1f} MB) received via direct HTTP stream from Main!")

        # 2. Build FFmpeg transcode command
        scale_filter = ref_cfg.get("scale_filter", "null")
        crf = ref_cfg.get("crf_val", 32)
        v_codec = ref_cfg.get("video_codec", "H.265 / HEVC")
        is_h264 = "264" in v_codec or "AVC" in v_codec
        has_nvenc = is_nvenc_available()

        if is_h264:
            if has_nvenc:
                v_args = ["-c:v", "h264_nvenc", "-preset", "p4", "-cq:v", str(crf)]
            else:
                v_args = ["-c:v", "libx264", "-crf", str(crf), "-preset", "veryfast"]
        else:
            if has_nvenc:
                v_args = ["-c:v", "hevc_nvenc", "-preset", "p4", "-cq:v", str(crf)]
            else:
                v_args = ["-c:v", "libx265", "-crf", str(crf), "-preset", "veryfast"]

        cmd = ["ffmpeg", "-nostdin", "-y"]
        # Direct VRAM hardware decoding via NVDEC: eliminates CPU decoding bottleneck
        if has_nvenc:
            cmd.extend(["-hwaccel", "cuda"])
        cmd.extend(["-i", local_raw])
        if scale_filter != "null":
            cmd.extend(["-vf", scale_filter])
        cmd.extend([
            *v_args,
            "-an",
            "-avoid_negative_ts", "make_zero",
            "-progress", "pipe:1",
            local_out
        ])

        dur_sec = float(task.get("dur_sec", 240.0))

        # 3. Launch FFmpeg process with live heartbeat monitor
        logger.notify("HELPER", f"⚡ Starting transcode of Chunk #{chunk_idx + 1} for Job #{job_id} ('{task.get('title')}') on {self.gpu_info['gpu_name']} (Slot {slot_id})...")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)

        last_hb_time = time.time()
        curr_pct = 0.0
        curr_fps = 0.0
        curr_speed = "--"

        try:
            for line in proc.stdout:
                line = line.strip()
                if line.startswith("fps="):
                    try:
                        curr_fps = float(line.split("=")[1].strip())
                        task["fps"] = curr_fps
                    except Exception:
                        pass
                elif line.startswith("speed="):
                    curr_speed = line.split("=")[1].strip()
                    task["speed"] = curr_speed
                elif line.startswith("out_time_us="):
                    try:
                        us = int(line.split("=")[1])
                        curr_sec = us / 1_000_000.0
                        curr_pct = min(99.0, max(0.0, (curr_sec / max(1.0, dur_sec)) * 100.0))
                        task["pct"] = curr_pct
                        task["fps"] = curr_fps
                        task["speed"] = curr_speed
                    except Exception:
                        pass
                    now = time.time()
                    if (now - last_hb_time) >= 10.0:
                        self.send_heartbeat({
                            "job_id": job_id,
                            "chunk_idx": chunk_idx,
                            "pct": curr_pct,
                            "fps": curr_fps,
                            "speed": curr_speed,
                            "status": "encoding"
                        })
                        logger.debug("HELPER", f"⚡ [Slot {slot_id}] Chunk #{chunk_idx + 1} progress: {curr_pct:.1f}% @ {curr_fps:.0f} fps | Speed: {curr_speed}")
                        last_hb_time = now
                elif line == "progress=end":
                    curr_pct = 100.0
                    task["pct"] = 100.0
            proc.wait()
        except Exception as e:
            proc.kill()
            logger.error("HELPER", f"[Slot {slot_id}] Exception during Chunk #{chunk_idx + 1} encoding: {e}")
            return False

        if proc.returncode != 0 or not os.path.exists(local_out) or os.path.getsize(local_out) == 0:
            logger.error("HELPER", f"❌ Chunk #{chunk_idx + 1} transcode failed on {self.gpu_info['gpu_name']} (Slot {slot_id}) [exit code: {proc.returncode}]")
            return False

        # 4. Copy completed chunk directly to Google Drive
        os.makedirs(os.path.dirname(out_drive_chunk), exist_ok=True)
        shutil.copyfile(local_out, out_drive_chunk)

        fsize_mb = os.path.getsize(out_drive_chunk) / (1024 * 1024)
        logger.success("HELPER", f"✅ Chunk #{chunk_idx + 1} transcode complete ({fsize_mb:.1f} MB) on {self.gpu_info['gpu_name']} (Slot {slot_id}). Saved to Drive.")

        # 5. Notify Main coordinator
        self.complete_task(job_id, chunk_idx, out_drive_chunk, fsize_mb)

        # 6. Clean local SSD temporary files
        for f in (local_raw, local_out):
            try:
                if os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass

        return True

    def _worker_slot_loop(self, slot_id: int):
        """Worker thread for a single GPU slot (max 2 slots per GPU)."""
        idle_ticks = 0
        while self.running:
            if not self.master_url:
                time.sleep(3)
                continue

            task = self.claim_task()
            if task:
                idle_ticks = 0
                with self.active_slots_lock:
                    self.active_slots[slot_id] = task
                self.announce_to_drive(status="encoding")
                success = self.encode_chunk(task, slot_id=slot_id)
                with self.active_slots_lock:
                    self.active_slots[slot_id] = None

                with self.active_slots_lock:
                    has_any_active = any(s is not None for s in self.active_slots.values())
                if not has_any_active:
                    self.announce_to_drive(status="idle")

                if not success:
                    time.sleep(3.0)
                else:
                    time.sleep(0.5)
            else:
                idle_ticks += 1
                if idle_ticks == 1 or idle_ticks % 20 == 0:
                    logger.debug("HELPER", f"[Slot {slot_id}] Idle: Waiting for available video chunks...")
                time.sleep(3)

    def run(self):
        """Main worker execution loop launching 2 concurrent GPU worker slots."""
        logger.notify("HELPER", "=" * 68)
        logger.notify("HELPER", f"🚀 Colab GPU Helper Node Started: {self.node_name}")
        logger.notify("HELPER", f"   Worker ID: {self.worker_id}")
        logger.notify("HELPER", f"   GPU: {self.gpu_info['gpu_name']} ({self.gpu_info['vram_mb']} MB VRAM)")
        logger.notify("HELPER", f"   NVENC Hardware Acceleration: {'Yes' if is_nvenc_available() else 'No (CPU Fallback)'}")
        logger.notify("HELPER", f"   Concurrency: {self.MAX_CONCURRENT_CHUNKS} chunks simultaneously per GPU")
        logger.notify("HELPER", f"   Debug Log Notifications: {'ENABLED (Verbose)' if logger.is_debug() else 'DISABLED (Milestones only)'}")
        logger.notify("HELPER", "=" * 68)

        self.announce_to_drive(status="idle")

        # Start 2 concurrent worker slots per GPU
        threads = []
        for slot in range(self.MAX_CONCURRENT_CHUNKS):
            t = threading.Thread(target=self._worker_slot_loop, args=(slot,), daemon=True)
            t.start()
            threads.append(t)

        while self.running:
            # 1. Discover or refresh Main coordinator URL
            if not self.master_url:
                self.master_url = self.discover_master_url()
                if self.master_url:
                    logger.notify("HELPER", f"🌐 Discovered Main Coordinator: {self.master_url}")
                    self.register_with_master()
                else:
                    logger.warn("HELPER", "⏳ Waiting for Main coordinator beacon on shared Google Drive (master.json)...")
                    self.announce_to_drive(status="waiting_for_master")
                    time.sleep(5)
                    continue

            # Periodic heartbeat to stay active and extend leases for all currently encoding slots
            with self.active_slots_lock:
                active_tasks = [t for t in self.active_slots.values() if t is not None]
            if active_tasks:
                for at in active_tasks:
                    self.send_heartbeat({
                        "job_id": at.get("job_id"),
                        "chunk_idx": at.get("chunk_idx"),
                        "pct": at.get("pct", 0.0),
                        "fps": at.get("fps", 0.0),
                        "speed": at.get("speed", "--"),
                        "status": "encoding"
                    })
            else:
                self.send_heartbeat({"status": "idle"})
            time.sleep(10)

def run_helper_main(enable_debug_logs: bool = True):
    """Entry point for Helper Colab notebook."""
    logger.set_debug(enable_debug_logs)
    worker = HelperWorker()
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.notify("HELPER", "🛑 Helper Worker interrupted by user. Shutting down...")
        worker.announce_to_drive(status="offline")

if __name__ == "__main__":
    run_helper_main()
