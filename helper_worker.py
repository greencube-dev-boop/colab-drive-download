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

def bust_fuse_path(file_path: str) -> None:
    """Traverses from Drive root down to file_path calling os.listdir to populate FUSE directory cache."""
    try:
        parts = []
        cur = os.path.dirname(file_path)
        while cur and cur != os.path.dirname(cur):
            parts.append(cur)
            cur = os.path.dirname(cur)
        for p in reversed(parts):
            if os.path.isdir(p):
                try:
                    os.listdir(p)
                except Exception:
                    pass
    except Exception:
        pass

class HelperWorker:
    """
    Autonomous GPU Refiner Worker node running inside a Colab helper notebook.
    Discovers Main via shared Drive, claims up to 2 concurrent video chunks,
    encodes via hardware NVENC, and returns completed segments directly to shared Drive.
    """
    MAX_CONCURRENT_CHUNKS = 2

    def __init__(self, worker_id: Optional[str] = None, coordinator_url: Optional[str] = None, drive_folder_name: Optional[str] = None):
        self.gpu_info = get_gpu_info()
        short_gpu = self.gpu_info["gpu_name"].replace("NVIDIA", "").replace("Tesla", "").strip() or "Node"
        self.worker_id = worker_id or f"worker_{short_gpu.lower()}_{uuid.uuid4().hex[:8]}"
        self.node_name = f"Helper ({self.gpu_info['gpu_name']})"
        self.master_url: Optional[str] = coordinator_url.strip().rstrip("/") if coordinator_url and coordinator_url.strip() else None
        self.running: bool = True
        self.active_slots: Dict[int, Optional[Dict[str, Any]]] = {0: None, 1: None}
        self.active_slots_lock = threading.Lock()
        self.failed_chunks_cooldown: Dict[Any, float] = {}

        # Resolve explicit drive folder if provided
        if drive_folder_name and drive_folder_name.strip():
            f_name = drive_folder_name.strip()
            candidates = [
                f_name,
                os.path.join("/content/drive/MyDrive", f_name),
                os.path.join("/content/drive/Shareddrives", f_name)
            ]
            for c in candidates:
                if os.path.isdir(c):
                    try:
                        config.update_downloader_dir(c)
                        break
                    except Exception:
                        pass
        # High-throughput RAM-disk staging: use /dev/shm if available and has >= 500 MB free space
        shm_path = "/dev/shm"
        self.using_ram_disk = False
        if os.path.exists(shm_path) and os.path.isdir(shm_path):
            try:
                stat = shutil.disk_usage(shm_path)
                if stat.free >= 500 * 1024 * 1024:
                    self.local_temp_dir = os.path.join(shm_path, "helper_work")
                    self.using_ram_disk = True
                else:
                    self.local_temp_dir = os.path.join(getattr(config, "LOCAL_WORKSPACE", "/content/temp"), "helper_work")
            except Exception:
                self.local_temp_dir = os.path.join(getattr(config, "LOCAL_WORKSPACE", "/content/temp"), "helper_work")
        else:
            self.local_temp_dir = os.path.join(getattr(config, "LOCAL_WORKSPACE", "/content/temp"), "helper_work")
        os.makedirs(self.local_temp_dir, exist_ok=True)

    def discover_master_url(self) -> Optional[str]:
        """
        Exhaustively searches for Main coordinator's master.json beacon across:
        1. config.CLUSTER_MASTER_FILE and config.LOCAL_CLUSTER_MASTER_FILE
        2. Current working directory (./data/cluster/master.json) and parent (../data/cluster/master.json)
        3. Colab Google Drive subfolders (/content/drive/MyDrive/*/data/cluster/master.json)
        4. Shortcuts (/content/drive/.shortcut-targets-by-id/*/data/cluster/master.json)
        5. Shared drives (/content/drive/Shareddrives/*/data/cluster/master.json)
        Forces Drive FUSE directory listing invalidation to prevent cached/missed files on Colab.
        """
        candidate_paths: List[str] = []

        # 1. Config target paths
        for attr in ["CLUSTER_MASTER_FILE", "LOCAL_CLUSTER_MASTER_FILE"]:
            p = getattr(config, attr, None)
            if p:
                candidate_paths.append(p)

        # 2. Local relative paths
        candidate_paths.append(os.path.abspath(os.path.join("data", "cluster", "master.json")))
        candidate_paths.append(os.path.abspath(os.path.join("..", "data", "cluster", "master.json")))

        # 3. Known /content/drive locations in Google Colab
        if getattr(config, "IS_COLAB", False):
            drive_roots = [
                "/content/drive/MyDrive",
                "/content/drive/Shareddrives",
                "/content/drive/.shortcut-targets-by-id"
            ]
            for droot in drive_roots:
                if not os.path.exists(droot):
                    continue
                try:
                    entries = os.listdir(droot) # Busts FUSE cache
                    for entry in entries:
                        sub_p = os.path.join(droot, entry)
                        if os.path.isdir(sub_p):
                            candidate_paths.append(os.path.join(sub_p, "data", "cluster", "master.json"))
                except Exception:
                    pass

        # De-duplicate preserving order
        seen = set()
        unique_candidates = []
        for cp in candidate_paths:
            norm = os.path.abspath(cp)
            if norm not in seen:
                seen.add(norm)
                unique_candidates.append(norm)

        for target in unique_candidates:
            # Recursively bust FUSE directory cache from root down to master.json
            parts = []
            cur = os.path.dirname(target)
            while cur and cur != os.path.dirname(cur):
                parts.append(cur)
                cur = os.path.dirname(cur)
            for p in reversed(parts):
                if os.path.isdir(p):
                    try:
                        os.listdir(p)
                    except Exception:
                        pass

            if os.path.exists(target):
                try:
                    with open(target, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    url = data.get("master_url")
                    if url and str(url).startswith("http"):
                        clean_url = str(url).rstrip("/")
                        # On Colab, ignore 127.0.0.1 / localhost as remote workers cannot reach coordinator locally
                        if getattr(config, "IS_COLAB", False) and ("127.0.0.1" in clean_url or "localhost" in clean_url):
                            continue
                        # Successfully discovered master beacon! Synchronize config.DOWNLOADER_DIR
                        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(target)))
                        if root_dir and os.path.exists(root_dir):
                            try:
                                config.update_downloader_dir(root_dir)
                            except Exception:
                                pass
                        return clean_url
                except Exception:
                    pass
        return None

    def _beacon_filename(self) -> str:
        return f"{self.worker_id}.json" if self.worker_id.startswith("worker_") else f"worker_{self.worker_id}.json"

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
        b_fname = self._beacon_filename()
        targets = [
            getattr(config, "CLUSTER_WORKERS_DIR", os.path.join(config.DATA_DIR, "cluster", "workers")),
            getattr(config, "LOCAL_CLUSTER_WORKERS_DIR", os.path.join(config.LOCAL_DATA_DIR, "cluster", "workers"))
        ]
        for tdir in targets:
            try:
                os.makedirs(tdir, exist_ok=True)
                w_file = os.path.join(tdir, b_fname)
                with open(w_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except Exception:
                        pass
            except Exception:
                pass

    def check_and_restore_announcer(self, status: str = "idle") -> bool:
        """
        Verifies that this worker's announcer beacon file exists on shared Google Drive.
        If it was deleted (e.g. by coordinator cleaning inactive nodes),
        re-creates it immediately and re-registers with the coordinator.
        """
        b_fname = self._beacon_filename()
        # Primary check on shared Google Drive cluster directory
        drive_tdir = getattr(config, "CLUSTER_WORKERS_DIR", os.path.join(config.DATA_DIR, "cluster", "workers"))
        is_missing = False
        try:
            os.makedirs(drive_tdir, exist_ok=True)
            try:
                os.listdir(drive_tdir)  # Invalidate Drive FUSE directory listing
            except Exception:
                pass
            w_file = os.path.join(drive_tdir, b_fname)
            if not os.path.exists(w_file):
                is_missing = True
        except Exception:
            pass

        if is_missing:
            logger.notify("HELPER", f"🔄 Announcer beacon was missing on Drive. Recreating '{b_fname}'...")
            self.announce_to_drive(status=status)
            if self.master_url:
                self.register_with_master()
            return True
        return False

    def cleanup_drive_beacon(self):
        """Removes this worker's beacon file from Google Drive on shutdown."""
        b_fname = self._beacon_filename()
        targets = [
            getattr(config, "CLUSTER_WORKERS_DIR", os.path.join(config.DATA_DIR, "cluster", "workers")),
            getattr(config, "LOCAL_CLUSTER_WORKERS_DIR", os.path.join(config.LOCAL_DATA_DIR, "cluster", "workers"))
        ]
        for tdir in targets:
            try:
                w_file = os.path.join(tdir, b_fname)
                if os.path.exists(w_file):
                    try:
                        config.bypass_trash_delete(w_file)
                    except Exception:
                        os.remove(w_file)
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
            "vram_mb": self.gpu_info["vram_mb"],
            "tunnel_url": getattr(self, "tunnel_url", "") or ""
        }
        logger.notify("HELPER", f"📡 Registering with Main coordinator ({self.master_url})...")
        res = self._http_request("/api/cluster/register", reg_payload)
        if res and res.get("status") == "registered":
            assigned = res.get("assigned_name", self.node_name)
            logger.success("HELPER", f"Successfully registered with Main coordinator as '{assigned}'!")

            # Synchronize Google Drive storage directory from Main coordinator's response
            main_folder_name = res.get("folder_name")
            main_dl_dir = res.get("downloader_dir")
            synced_dir = None
            if getattr(config, "IS_COLAB", False):
                if main_folder_name:
                    roots = ["/content/drive/MyDrive", "/content/drive/Shareddrives"]
                    for droot in roots:
                        if os.path.isdir(droot):
                            try:
                                for entry in os.listdir(droot):
                                    if entry.strip().lower() == main_folder_name.strip().lower():
                                        cand = os.path.join(droot, entry)
                                        if os.path.isdir(cand):
                                            synced_dir = cand
                                            break
                            except Exception:
                                pass
                        if synced_dir:
                            break
                if not synced_dir and main_dl_dir and os.path.isdir(main_dl_dir):
                    synced_dir = main_dl_dir
            if synced_dir:
                try:
                    config.update_downloader_dir(synced_dir)
                    logger.notify("HELPER", f"📁 Synchronized Drive workspace folder with Main: {synced_dir}")
                except Exception:
                    pass
        else:
            logger.warn("HELPER", "Direct HTTP registration pending; synced via Google Drive shared beacon.")

        self.announce_to_drive(status="idle")
        return True

    def send_heartbeat(self, metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Sends live progress and heartbeat to Main."""
        payload = metrics or {}
        payload["worker_id"] = self.worker_id
        res = self._http_request("/api/cluster/heartbeat", payload) or {}
        st = payload.get("status", "idle")
        self.check_and_restore_announcer(status=st)
        if payload.get("status") == "encoding":
            logger.debug("HELPER", f"💓 Heartbeat: Chunk #{payload.get('chunk_idx')} ({payload.get('pct', 0.0):.1f}%) @ {payload.get('fps', 0)} fps")
        return res

    def start_tunnel_and_server(self, port: int = 7861):
        """Spawns local Cloudflare tunnel and starts a lightweight worker HTTP server."""
        try:
            import tunnel
            self.tunnel_url = tunnel.start_cloudflare_tunnel(port=port)
            if self.tunnel_url:
                logger.notify("HELPER", f"🌐 Helper Cloudflare Tunnel active: {self.tunnel_url}")
        except Exception as e:
            logger.debug("HELPER", f"Notice: Could not start helper Cloudflare tunnel: {e}")

        # Start lightweight HTTP server for coordinator health-checks / abort signals
        try:
            import http.server
            worker_instance = self

            class _WorkerHandler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    if "/health" in self.path:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        res = {"status": "ok", "worker_id": worker_instance.worker_id}
                        self.wfile.write(json.dumps(res).encode("utf-8"))
                    else:
                        self.send_response(200)
                        self.end_headers()

                def do_POST(self):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status":"ok"}')

                def log_message(self, format, *args):
                    pass

            from http.server import ThreadingHTTPServer
            self._httpd = ThreadingHTTPServer(("0.0.0.0", port), _WorkerHandler)
            srv_thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
            srv_thread.start()
        except Exception as e:
            logger.debug("HELPER", f"Notice: Worker local HTTP server could not bind to port {port}: {e}")

    def signal_slot_free(self, slot_id: int = 0) -> Optional[Dict[str, Any]]:
        """Signals Main that this GPU slot is idle and requests next chunk assignment from Priority Queue."""
        now = time.time()
        active_cooldowns = [
            f"{j}:{c}" for (j, c), exp in self.failed_chunks_cooldown.items() if now < exp
        ]
        payload = {
            "worker_id": self.worker_id,
            "slot_id": slot_id,
            "node_name": self.node_name,
            "exclude_chunks": active_cooldowns
        }
        res = self._http_request("/api/cluster/signal_free", payload)
        if res and res.get("has_task"):
            j_id = res.get("job_id", 0)
            c_idx = res.get("chunk_idx", -1)
            cooldown_until = self.failed_chunks_cooldown.get((j_id, c_idx), 0.0)
            if time.time() < cooldown_until:
                self.release_task(j_id, c_idx)
                time.sleep(2.0)
                return None
            logger.notify("HELPER", f"📥 Claimed Chunk #{res.get('chunk_idx', 0) + 1}/{res.get('total_chunks')} of Job #{res.get('job_id')} ('{res.get('title', 'Job')}')")
            return res
        return None

    def claim_task(self) -> Optional[Dict[str, Any]]:
        """Polls Main for next available chunk assignment."""
        now = time.time()
        active_cooldowns = [
            f"{j}:{c}" for (j, c), exp in self.failed_chunks_cooldown.items() if now < exp
        ]
        cooldown_param = f"&exclude_chunks={','.join(active_cooldowns)}" if active_cooldowns else ""
        endpoint = f"/api/cluster/claim_task?worker_id={self.worker_id}&node_name={urllib.parse.quote(self.node_name)}{cooldown_param}"
        res = self._http_request(endpoint)
        if res and res.get("has_task"):
            j_id = res.get("job_id", 0)
            c_idx = res.get("chunk_idx", -1)
            # Check if this worker recently failed this exact chunk
            cooldown_until = self.failed_chunks_cooldown.get((j_id, c_idx), 0.0)
            if time.time() < cooldown_until:
                self.release_task(j_id, c_idx)
                time.sleep(2.0)
                return None
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
        return bool(res and res.get("status") in ["accepted", "already_done"])

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
        """Downloads raw chunk file directly from Main coordinator via HTTP stream with strict timeout and chunked reads."""
        if not self.master_url:
            return False
        url = f"{self.master_url.rstrip('/')}{endpoint}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "X-Worker-ID": self.worker_id
        }
        req = urllib.request.Request(url, headers=headers)
        start_t = time.time()
        max_duration_sec = 45.0
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                if resp.status != 200:
                    return False
                with open(target_local_path, "wb") as out_f:
                    while True:
                        if (time.time() - start_t) > max_duration_sec:
                            logger.warn("HELPER", f"HTTP chunk download timed out after {max_duration_sec}s ({url})")
                            break
                        buf = resp.read(128 * 1024)
                        if not buf:
                            break
                        out_f.write(buf)
            if os.path.exists(target_local_path) and os.path.getsize(target_local_path) > 0:
                return True
            if os.path.exists(target_local_path):
                try:
                    os.remove(target_local_path)
                except Exception:
                    pass
            return False
        except Exception as e:
            logger.debug("HELPER", f"HTTP chunk stream download error ({url}): {e}")
            if os.path.exists(target_local_path):
                try:
                    os.remove(target_local_path)
                except Exception:
                    pass
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

        # 1. Fetch raw segment: Check Google Drive with deep cache invalidation & patience (up to 30s)
        found_on_drive = False
        candidate_paths = [raw_drive_chunk]
        # Also check with local config.DOWNLOADER_DIR if different from coordinator's base path
        chunk_fname = os.path.basename(raw_drive_chunk)
        local_cand = os.path.join(getattr(config, "CHUNKS_STAGING_DIR", os.path.join(config.DOWNLOADER_DIR, "staging", "chunks")), f"job_{job_id}", chunk_fname)
        if local_cand not in candidate_paths:
            candidate_paths.append(local_cand)

        max_drive_attempts = 20
        for attempt in range(max_drive_attempts):
            for cand in candidate_paths:
                bust_fuse_path(cand)
                if os.path.exists(cand) and os.path.getsize(cand) > 0:
                    raw_drive_chunk = cand
                    found_on_drive = True
                    break
            if found_on_drive:
                break
            if (attempt + 1) % 4 == 0:
                logger.notify("HELPER", f"⏳ [Slot {slot_id}] Waiting for Drive FUSE cloud sync for Chunk #{chunk_idx + 1} ({int((attempt + 1) * 1.5)}s elapsed)...")
            time.sleep(1.5)

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
                self.failed_chunks_cooldown[(job_id, chunk_idx)] = time.time() + 25.0
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
                    if (now - last_hb_time) >= 2.0:
                        prog_res = self._http_request("/api/cluster/chunk_progress", {
                            "worker_id": self.worker_id,
                            "job_id": job_id,
                            "chunk_idx": chunk_idx,
                            "pct": curr_pct,
                            "fps": curr_fps,
                            "speed": curr_speed
                        })
                        if isinstance(prog_res, dict) and prog_res.get("abort_chunk"):
                            logger.notify("HELPER", f"🛑 [Slot {slot_id}] Chunk #{chunk_idx + 1} aborted by coordinator (winner completed). Freeing slot...")
                            proc.terminate()
                            try:
                                proc.wait(timeout=2.0)
                            except Exception:
                                proc.kill()
                            for f in (local_raw, local_out):
                                try:
                                    if os.path.exists(f):
                                        os.remove(f)
                                except Exception:
                                    pass
                            return True
                        logger.debug("HELPER", f"⚡ [Slot {slot_id}] Chunk #{chunk_idx + 1} progress: {curr_pct:.1f}% @ {curr_fps:.0f} fps | Speed: {curr_speed}")
                        last_hb_time = now
                elif line == "progress=end":
                    curr_pct = 100.0
                    task["pct"] = 100.0
            proc.wait()
        except Exception as e:
            proc.kill()
            logger.error("HELPER", f"[Slot {slot_id}] Exception during Chunk #{chunk_idx + 1} encoding: {e}")
            self.failed_chunks_cooldown[(job_id, chunk_idx)] = time.time() + 20.0
            self.release_task(job_id, chunk_idx)
            return False

        if proc.returncode != 0 or not os.path.exists(local_out) or os.path.getsize(local_out) == 0:
            logger.error("HELPER", f"❌ Chunk #{chunk_idx + 1} transcode failed on {self.gpu_info['gpu_name']} (Slot {slot_id}) [exit code: {proc.returncode}]")
            self.failed_chunks_cooldown[(job_id, chunk_idx)] = time.time() + 20.0
            self.release_task(job_id, chunk_idx)
            return False

        # Clear cooldown on success
        self.failed_chunks_cooldown.pop((job_id, chunk_idx), None)

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

            task = self.signal_slot_free(slot_id=slot_id) or self.claim_task()
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

        # 0. Start local Cloudflare tunnel & lightweight HTTP health/control server
        self.start_tunnel_and_server(port=7861)

        # Start 2 concurrent worker slots per GPU
        threads = []
        for slot in range(self.MAX_CONCURRENT_CHUNKS):
            t = threading.Thread(target=self._worker_slot_loop, args=(slot,), daemon=True)
            t.start()
            threads.append(t)

        if self.master_url:
            logger.notify("HELPER", f"🌐 Coordinator Endpoint Configured: {self.master_url}")
            self.register_with_master()

        wait_ticks = 0
        while self.running:
            # 1. Discover or refresh Main coordinator URL
            if not self.master_url:
                self.master_url = self.discover_master_url()
                if self.master_url:
                    logger.notify("HELPER", f"🌐 Discovered Main Coordinator: {self.master_url}")
                    self.register_with_master()
                else:
                    wait_ticks += 1
                    if wait_ticks == 1 or wait_ticks % 6 == 0:
                        logger.warn("HELPER", "⏳ Waiting for Main coordinator beacon on shared Google Drive (master.json)...")
                        logger.notify("HELPER", "👉 Tip: If auto-discovery takes time, paste the Main coordinator URL into the 'coordinator_url' parameter in Step 2.")
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

def run_helper_main(coordinator_url: str = "", drive_folder_name: str = "", enable_debug_logs: bool = True):
    """Entry point for Helper Colab notebook."""
    logger.set_debug(enable_debug_logs)
    worker = HelperWorker(coordinator_url=coordinator_url, drive_folder_name=drive_folder_name)
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.notify("HELPER", "🛑 Helper Worker interrupted by user. Shutting down...")
        try:
            worker.announce_to_drive(status="offline")
            worker.cleanup_drive_beacon()
        except Exception:
            pass

if __name__ == "__main__":
    run_helper_main()
