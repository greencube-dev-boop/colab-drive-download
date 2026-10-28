import os
import sys
import time
import json
import uuid
import shutil
import urllib.request
import urllib.error
import subprocess
import threading
from typing import Dict, Any, Optional

import config

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
    Discovers Main via shared Drive, claims video chunks, encodes via NVENC,
    and returns completed segments directly to shared Drive.
    """
    def __init__(self, worker_id: Optional[str] = None):
        self.gpu_info = get_gpu_info()
        short_gpu = self.gpu_info["gpu_name"].replace("NVIDIA", "").replace("Tesla", "").strip() or "Node"
        self.worker_id = worker_id or f"worker_{short_gpu.lower()}_{uuid.uuid4().hex[:8]}"
        self.node_name = f"Helper ({self.gpu_info['gpu_name']})"
        self.master_url: Optional[str] = None
        self.running: bool = True
        self.current_task: Optional[Dict[str, Any]] = None
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
        """Writes collision-free worker beacon file to shared Google Drive."""
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
                temp_file = w_file + ".tmp"
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                try:
                    os.replace(temp_file, w_file)
                except OSError:
                    shutil.copyfile(temp_file, w_file)
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
            except Exception:
                pass

    def _http_request(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Sends an HTTP POST or GET request to the Main coordinator API."""
        if not self.master_url:
            return None
        url = f"{self.master_url}{endpoint}"
        try:
            req_data = None
            headers = {"User-Agent": f"ColabHelperWorker/{self.worker_id}"}
            if data is not None:
                req_data = json.dumps(data).encode("utf-8")
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(url, data=req_data, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                if resp.status == 200:
                    body = resp.read().decode("utf-8")
                    return json.loads(body)
        except Exception:
            return None
        return None

    def register_with_master(self) -> bool:
        """Registers worker directly with Main via HTTP API."""
        reg_payload = {
            "worker_id": self.worker_id,
            "node_name": self.node_name,
            "gpu_name": self.gpu_info["gpu_name"],
            "vram_mb": self.gpu_info["vram_mb"]
        }
        res = self._http_request("/api/cluster/register", reg_payload)
        return bool(res and res.get("status") == "registered")

    def send_heartbeat(self, metrics: Optional[Dict[str, Any]] = None) -> bool:
        """Sends live progress and heartbeat to Main."""
        payload = metrics or {}
        payload["worker_id"] = self.worker_id
        res = self._http_request("/api/cluster/heartbeat", payload)
        self.announce_to_drive(status=payload.get("status", "idle"))
        return bool(res and res.get("status") == "ok")

    def claim_task(self) -> Optional[Dict[str, Any]]:
        """Polls Main for next available chunk assignment."""
        endpoint = f"/api/cluster/claim_task?worker_id={self.worker_id}"
        res = self._http_request(endpoint)
        if res and res.get("has_task"):
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
        return bool(res and res.get("status") == "accepted")

    def encode_chunk(self, task: Dict[str, Any]) -> bool:
        """
        Executes hardware-accelerated NVENC transcode on local SSD copy of chunk,
        sending periodic heartbeats, then copies finished .mkv to shared Drive.
        """
        raw_drive_chunk = task["raw_chunk_path"]
        out_drive_chunk = task["out_chunk_path"]
        ref_cfg = task.get("refining_config", {})
        job_id = task["job_id"]
        chunk_idx = task["chunk_idx"]

        if not os.path.exists(raw_drive_chunk):
            print(f"[HelperWorker] Error: Source chunk does not exist on Drive: {raw_drive_chunk}")
            return False

        chunk_name = os.path.basename(raw_drive_chunk)
        local_raw = os.path.join(self.local_temp_dir, f"in_{chunk_name}")
        local_out = os.path.join(self.local_temp_dir, f"out_{chunk_name}")

        for f in (local_raw, local_out):
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

        # 1. Copy raw segment to local fast SSD
        shutil.copyfile(raw_drive_chunk, local_raw)

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

        cmd = ["ffmpeg", "-nostdin", "-y", "-i", local_raw]
        if scale_filter != "null":
            cmd.extend(["-vf", scale_filter])
        cmd.extend([
            *v_args,
            "-an",
            "-avoid_negative_ts", "make_zero",
            "-progress", "pipe:1",
            local_out
        ])

        # 3. Launch FFmpeg process with live heartbeat monitor
        print(f"[HelperWorker] Encoding Chunk #{chunk_idx} of Job #{job_id} on {self.gpu_info['gpu_name']}...")
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
                    except Exception:
                        pass
                elif line.startswith("speed="):
                    curr_speed = line.split("=")[1].strip()
                elif line.startswith("out_time_us="):
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
                        last_hb_time = now
            proc.wait()
        except Exception as e:
            proc.kill()
            print(f"[HelperWorker] Exception during encoding: {e}")
            return False

        if proc.returncode != 0 or not os.path.exists(local_out) or os.path.getsize(local_out) == 0:
            print(f"[HelperWorker] FFmpeg transcode failed (exit code: {proc.returncode})")
            return False

        # 4. Copy completed chunk atomically to Google Drive
        os.makedirs(os.path.dirname(out_drive_chunk), exist_ok=True)
        part_drive = out_drive_chunk + ".part"
        shutil.copyfile(local_out, part_drive)
        try:
            os.replace(part_drive, out_drive_chunk)
        except OSError:
            shutil.copyfile(part_drive, out_drive_chunk)
            if os.path.exists(part_drive):
                os.remove(part_drive)

        fsize_mb = os.path.getsize(out_drive_chunk) / (1024 * 1024)
        print(f"[HelperWorker] ✅ Chunk #{chunk_idx} finished successfully ({fsize_mb:.1f} MB). Submitting...")

        # 5. Notify Main
        self.complete_task(job_id, chunk_idx, out_drive_chunk, fsize_mb)

        # 6. Clean local temp
        for f in (local_raw, local_out):
            try:
                if os.path.exists(f):
                    os.remove(f)
            except Exception:
                pass

        return True

    def run(self):
        """Main worker execution loop."""
        print("=" * 72)
        print(f"🚀 Colab GPU Helper Node Started: {self.node_name}")
        print(f"   Worker ID: {self.worker_id}")
        print(f"   GPU Detected: {self.gpu_info['gpu_name']} ({self.gpu_info['vram_mb']} MB VRAM)")
        print(f"   NVENC Support: {'Yes (Hardware Accelerated)' if is_nvenc_available() else 'No (CPU Fallback)'}")
        print("=" * 72)

        self.announce_to_drive(status="idle")

        while self.running:
            # 1. Discover or refresh Main coordinator URL
            if not self.master_url:
                self.master_url = self.discover_master_url()
                if self.master_url:
                    print(f"🌐 Discovered Main Coordinator: {self.master_url}")
                    self.register_with_master()
                else:
                    print("⏳ Waiting for Main coordinator beacon on shared Google Drive (master.json)...")
                    self.announce_to_drive(status="waiting_for_master")
                    time.sleep(5)
                    continue

            # 2. Try to claim next available chunk
            task = self.claim_task()
            if task:
                self.current_task = task
                self.announce_to_drive(status="encoding")
                success = self.encode_chunk(task)
                self.current_task = None
                self.announce_to_drive(status="idle")
                time.sleep(0.5)
            else:
                self.send_heartbeat({"status": "idle"})
                time.sleep(4)

def run_helper_main():
    worker = HelperWorker()
    try:
        worker.run()
    except KeyboardInterrupt:
        print("\n🛑 Helper Worker interrupted by user. Shutting down...")
        worker.announce_to_drive(status="offline")

if __name__ == "__main__":
    run_helper_main()
