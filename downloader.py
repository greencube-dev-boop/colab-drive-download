import os
import re
import json
import time
import urllib.parse
import urllib.request
import subprocess
import threading
try:
    import requests
except ImportError:
    requests = None
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional
import config
from config import RAW_DIR, LOCAL_RAW_DIR, TEMP_DIR, tasks_lock

def _kill_proc(proc: Optional[subprocess.Popen]):
    """Forcefully terminates a subprocess and its child process tree."""
    if not proc:
        return
    try:
        proc.terminate()
        proc.kill()
    except Exception:
        pass
    try:
        if os.name == 'nt':
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            import signal
            try:
                pgid = os.getpgid(proc.pid)
                if pgid != os.getpgrp():
                    os.killpg(pgid, signal.SIGKILL)
                else:
                    os.kill(proc.pid, signal.SIGKILL)
            except Exception:
                try:
                    os.kill(proc.pid, signal.SIGKILL)
                except Exception:
                    pass
    except Exception:
        pass

def parse_speed_to_bps(speed_str: str) -> float:
    """Parses speed strings (e.g. '54KiB/s', '1.2MiB/s', '350KB/s') to bytes per second."""
    if not speed_str or speed_str == "--":
        return 0.0
    m = re.match(r'([\d.]+)\s*([KMGT]?)(?:i?B)?', speed_str.strip(), re.I)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = m.group(2).upper()
    if unit == 'K':
        return val * 1024.0
    elif unit == 'M':
        return val * 1024.0 * 1024.0
    elif unit == 'G':
        return val * 1024.0 * 1024.0 * 1024.0
    elif unit == 'T':
        return val * 1024.0 * 1024.0 * 1024.0 * 1024.0
    return val

def parse_size_to_bytes(size_str: str) -> int:
    """Parses size strings (e.g. '12.5MiB', '500KiB') to integer bytes."""
    if not size_str or size_str == "--":
        return 0
    m = re.match(r'([\d.]+)\s*([KMGT]?)(?:i?B)?', size_str.strip(), re.I)
    if not m:
        return 0
    val = float(m.group(1))
    unit = m.group(2).upper()
    if unit == 'K':
        return int(val * 1024)
    elif unit == 'M':
        return int(val * 1024 * 1024)
    elif unit == 'G':
        return int(val * 1024 * 1024 * 1024)
    elif unit == 'T':
        return int(val * 1024 * 1024 * 1024 * 1024)
    return int(val)

class DownloadWatchdog:
    """
    Monitors download process runtime speed and activity:
    1. If download does not start within timeout_start_sec (default 90s), triggers start timeout.
    2. If download speed remains continuously under low_speed_threshold_bps (default 100 KB/s)
       for low_speed_sec (default 120s), triggers demotion to the low-speed queue.
       (Only applies if task is not already in the low-speed queue).
    """
    def __init__(
        self,
        task: Dict[str, Any],
        timeout_start_sec: Optional[float] = None,
        low_speed_threshold_bps: Optional[float] = None,
        low_speed_sec: Optional[float] = None
    ):
        self.task = task
        self.timeout_start_sec = timeout_start_sec if timeout_start_sec is not None else getattr(config, "DOWNLOAD_START_TIMEOUT_SEC", 90.0)
        self.low_speed_threshold_bps = low_speed_threshold_bps if low_speed_threshold_bps is not None else getattr(config, "DOWNLOAD_LOW_SPEED_THRESHOLD_BPS", 100 * 1024.0)
        self.low_speed_sec = low_speed_sec if low_speed_sec is not None else getattr(config, "DOWNLOAD_LOW_SPEED_TIMEOUT_SEC", 120.0)
        self.t_start = time.time()
        self.low_speed_start: Optional[float] = None
        self.download_started = False
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None

    def start(self):
        self.t_start = time.time()
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def update_progress(self, current_bytes: int, speed_bps: float):
        if current_bytes > 0 or speed_bps > 0:
            if not self.download_started:
                self.download_started = True
                self.task["download_started"] = True

        if self.download_started:
            if speed_bps < self.low_speed_threshold_bps:
                if self.low_speed_start is None:
                    self.low_speed_start = time.time()
            else:
                self.low_speed_start = None

    def check_conditions(self, now: Optional[float] = None) -> bool:
        """Evaluates watchdog rules at timestamp `now`. Returns True if timeout/demote condition triggered."""
        if now is None:
            now = time.time()
        if self.task.get("cancelled") or self.task.get("timed_out_start") or self.task.get("demote_low_speed"):
            return False

        # 1. Start timeout check (90s)
        if not self.download_started:
            elapsed = now - self.t_start
            if elapsed >= self.timeout_start_sec:
                print(f"[Watchdog] Task #{self.task.get('id')} '{self.task.get('title')}' did not start within {self.timeout_start_sec}s. Aborting for retry.")
                self.task["timed_out_start"] = True
                self.task["status"] = f"Start Timeout ({self.timeout_start_sec}s)"
                proc = self.task.get("active_proc")
                if proc:
                    _kill_proc(proc)
                for p in list(self.task.get("active_procs", [])):
                    _kill_proc(p)
                return True

        # 2. Continuous low speed check (< 100 KB/s for 120s)
        # Only demote if the task is NOT already in the low-speed queue
        elif not self.task.get("is_low_speed", False):
            if self.low_speed_start is not None:
                duration = now - self.low_speed_start
                if duration >= self.low_speed_sec:
                    print(f"[Watchdog] Task #{self.task.get('id')} '{self.task.get('title')}' speed under 100 KB/s for {duration:.1f}s. Demoting to Low-Speed Queue.")
                    self.task["demote_low_speed"] = True
                    self.task["status"] = "Demoting to Low-Speed Queue (< 100 KB/s)"
                    proc = self.task.get("active_proc")
                    if proc:
                        _kill_proc(proc)
                    for p in list(self.task.get("active_procs", [])):
                        _kill_proc(p)
                    return True

        return False

    def _run(self):
        while not self.stop_event.wait(1.0):
            if self.check_conditions():
                break

def unmask_ts(d: bytes) -> bytes:
    """Strips PNG camouflage prefix used by video CDN hosts."""
    if len(d) > 400 and d[:4] == b'\x89PNG':
        lim = min(len(d) - 377, 65536)
        for k in range(8, lim):
            if d[k] == 0x47 and d[k + 188] == 0x47 and d[k + 376] == 0x47:
                return d[k:]
    return d

def download_hls_stream(stream_url: str, headers: Dict[str, str], raw_path: str, task: Dict[str, Any], videm_meta: Optional[Dict[str, Any]] = None):
    """Resilient HLS Downloader with sliding window, retries, and token refresh."""
    content = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(stream_url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                content = resp.read().decode('utf-8', errors='ignore')
            if content and ('#EXT' in content or len(content) > 10):
                break
        except Exception as e:
            if attempt == 2:
                raise ValueError(f"Failed to fetch master M3U8 playlist after 3 attempts: {e}")
            time.sleep(1.5)

    variant_url = stream_url
    if content and '#EXT-X-STREAM-INF' in content:
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        best_bw = -1
        best_var = None
        cur_bw = 0
        for line in lines:
            if line.startswith('#EXT-X-STREAM-INF:'):
                bw_match = re.search(r'BANDWIDTH=(\d+)', line)
                cur_bw = int(bw_match.group(1)) if bw_match else 0
            elif not line.startswith('#'):
                var_cand = urllib.parse.urljoin(stream_url, line)
                if cur_bw >= best_bw:
                    best_bw = cur_bw
                    best_var = var_cand
        variant_url = best_var or stream_url

    v_content = None
    for attempt in range(3):
        try:
            v_req = urllib.request.Request(variant_url, headers=headers)
            with urllib.request.urlopen(v_req, timeout=20) as v_resp:
                v_content = v_resp.read().decode('utf-8', errors='ignore')
            if v_content and ('#EXT' in v_content or len(v_content) > 10):
                break
        except Exception as e:
            if attempt == 2:
                raise ValueError(f"Failed to fetch variant M3U8 playlist after 3 attempts: {e}")
            time.sleep(1.5)

    segments = [urllib.parse.urljoin(variant_url, line.strip())
                for line in v_content.splitlines()
                if line.strip() and not line.startswith('#')]

    total_segs = len(segments)
    if total_segs == 0:
        raise ValueError("Stream playlist contains 0 video segments")

    task["total_str"] = f"{total_segs} segs"

    def refresh_videm_segments():
        if not videm_meta:
            return None
        try:
            refs_to_try = [videm_meta['ref']] + [s['ref'] for s in videm_meta.get('backup_servers', []) if s.get('ref') and s['ref'] != videm_meta['ref']]
            for ref in refs_to_try:
                try:
                    play_refresh_url = f"https://videm.xyz/api.php?a=play&ref={urllib.parse.quote(ref)}&t={urllib.parse.quote(videm_meta['t'])}&fresh=1"
                    pr_req = urllib.request.Request(play_refresh_url, headers=videm_meta['api_headers'])
                    with urllib.request.urlopen(pr_req, timeout=10) as pr_resp:
                        pr_data = json.loads(pr_resp.read().decode('utf-8'))
                    cand_u = pr_data.get('url')
                    if not cand_u:
                        continue
                    n_stream = urllib.parse.urljoin('https://videm.xyz/', cand_u)
                    n_variant = n_stream
                    n_mreq = urllib.request.Request(n_stream, headers=videm_meta['headers'])
                    with urllib.request.urlopen(n_mreq, timeout=10) as nm_resp:
                        nm_cont = nm_resp.read().decode('utf-8', errors='ignore')
                    if '#EXT-X-STREAM-INF' in nm_cont:
                        n_lines = [l.strip() for l in nm_cont.splitlines() if l.strip()]
                        b_bw = -1
                        b_var = n_stream
                        c_bw = 0
                        for l in n_lines:
                            if l.startswith('#EXT-X-STREAM-INF:'):
                                m = re.search(r'BANDWIDTH=(\d+)', l)
                                c_bw = int(m.group(1)) if m else 0
                            elif not l.startswith('#'):
                                cand = urllib.parse.urljoin(n_stream, l)
                                if c_bw >= b_bw:
                                    b_bw = c_bw
                                    b_var = cand
                        n_variant = b_var
                    nv_req = urllib.request.Request(n_variant, headers=videm_meta['headers'])
                    with urllib.request.urlopen(nv_req, timeout=10) as nv_resp:
                        nv_cont = nv_resp.read().decode('utf-8', errors='ignore')
                    segs = [urllib.parse.urljoin(n_variant, l.strip()) for l in nv_cont.splitlines() if l.strip() and not l.startswith('#')]
                    if segs:
                        return segs
                except Exception:
                    continue
            return None
        except Exception:
            return None

    MAX_WORKERS = min(16, max(2, int(task.get("download_connections", 8))))
    SLIDING_WINDOW = MAX_WORKERS * 3

    def fetch_seg_resilient(idx, seg_url):
        curr_url = seg_url
        for attempt in range(5):
            if task.get("cancelled"):
                return b''
            try:
                sreq = urllib.request.Request(curr_url, headers=headers)
                with urllib.request.urlopen(sreq, timeout=15) as sresp:
                    data = sresp.read()
                return unmask_ts(data)
            except Exception as err:
                if ('403' in str(err) or 'Forbidden' in str(err)) and videm_meta and attempt <= 2:
                    new_segs = refresh_videm_segments()
                    if new_segs and idx < len(new_segs):
                        curr_url = new_segs[idx]
                        segments[idx:] = new_segs[idx:]
                time.sleep(0.5 * (attempt + 1))
        return b''

    futures = {}
    next_submit_idx = 0
    next_write_idx = 0
    total_bytes = 0
    t_start = time.time()
    t_prev = t_start
    bytes_prev = 0

    watchdog = DownloadWatchdog(task)
    watchdog.start()

    try:
        with open(raw_path, "wb") as f_out:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                while next_write_idx < total_segs:
                    if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
                        break

                    while next_submit_idx < total_segs and (next_submit_idx - next_write_idx) < SLIDING_WINDOW:
                        s_idx = next_submit_idx
                        s_url = segments[s_idx]
                        futures[s_idx] = pool.submit(fetch_seg_resilient, s_idx, s_url)
                        next_submit_idx += 1

                    if next_write_idx in futures:
                        fut = futures[next_write_idx]
                        try:
                            chunk = fut.result(timeout=20)
                        except Exception:
                            chunk = b''
                        del futures[next_write_idx]

                        if chunk:
                            f_out.write(chunk)
                            total_bytes += len(chunk)

                        next_write_idx += 1

                        t_now = time.time()
                        if t_now - t_prev >= 0.5 or next_write_idx == total_segs:
                            spd_bps = (total_bytes - bytes_prev) / max(0.001, t_now - t_prev)
                            spd_mb = spd_bps / (1024 * 1024)
                            pct = (next_write_idx / total_segs) * 100
                            task["download_pct"] = pct
                            task["download_speed"] = f"{spd_mb:.1f} MB/s"
                            task["downloaded_str"] = f"{total_bytes / (1024*1024):.1f} MB"
                            task["total_str"] = f"{next_write_idx}/{total_segs} segs"
                            watchdog.update_progress(total_bytes, spd_bps)
                            t_prev = t_now
                            bytes_prev = total_bytes
                    else:
                        time.sleep(0.02)
    finally:
        watchdog.stop()

    if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
        return

    task["download_pct"] = 100.0
    if os.path.exists(raw_path):
        task["raw_size_mb"] = os.path.getsize(raw_path) / (1024 * 1024)

def download_ytdlp(url: str, raw_path: str, headers: Dict[str, str], task: Dict[str, Any]):
    """Downloads streams via yt-dlp with real-time stdout progress parsing."""
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--newline",
        "--concurrent-fragments", "8",
        "-o", raw_path,
        url
    ]
    if headers.get('Referer'):
        cmd.extend(["--add-header", f"Referer:{headers['Referer']}"])
    if headers.get('User-Agent'):
        cmd.extend(["--user-agent", headers['User-Agent']])

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    task["active_proc"] = proc
    task.setdefault("active_procs", []).append(proc)
    watchdog = DownloadWatchdog(task)
    watchdog.start()
    pat = re.compile(r'\[download\]\s+([\d.]+)%\s+of\s+~?([\d.]+[KMGT]i?B)\s+at\s+([\d.]+[KMGT]i?B/s)')
    try:
        for line in proc.stdout:
            if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
                _kill_proc(proc)
                return
            m = pat.search(line)
            if m:
                task["download_pct"] = float(m.group(1))
                task["total_str"] = m.group(2)
                task["download_speed"] = m.group(3)
                task["downloaded_str"] = f"{float(m.group(1)):.1f}%"
                watchdog.update_progress(parse_size_to_bytes(m.group(2)), parse_speed_to_bps(m.group(3)))
        proc.wait()
    finally:
        watchdog.stop()
        if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed") or (proc and proc.poll() is None):
            _kill_proc(proc)
        task["active_proc"] = None
        if proc in task.get("active_procs", []):
            task["active_procs"].remove(proc)

    if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
        return
    if proc.returncode != 0 or not os.path.exists(raw_path):
        raise RuntimeError(f"yt-dlp download failed with exit code {proc.returncode}")

    task["download_pct"] = 100.0
    task["raw_size_mb"] = os.path.getsize(raw_path) / (1024 * 1024)

def download_aria2c_or_requests(url: str, raw_path: str, task: Dict[str, Any]):
    """Multi-connection download using aria2c (16 connections) with requests fallback."""
    raw_dir = os.path.dirname(raw_path)
    target_name = os.path.basename(raw_path)

    conn_str = str(task.get("download_connections", 16))
    aria_cmd = [
        "aria2c",
        "-c",
        "-x", conn_str,
        "-s", conn_str,
        "-k", "1M",
        "--file-allocation=none",
        "--summary-interval=1",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "-d", raw_dir,
        "-o", target_name,
        url
    ]

    aria_success = False
    proc = None
    watchdog = DownloadWatchdog(task)
    watchdog.start()
    try:
        proc = subprocess.Popen(aria_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        task["active_proc"] = proc
        task.setdefault("active_procs", []).append(proc)
        pattern = re.compile(r'([\d.]+[KMGT]iB)/([\d.]+[KMGT]iB)\s*\((\d+)%\).*DL:([\d.]+[KMGT]iB)')
        for line in proc.stdout:
            if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
                _kill_proc(proc)
                return
            m = pattern.search(line)
            if m:
                task["downloaded_str"] = m.group(1)
                task["total_str"] = m.group(2)
                task["download_pct"] = float(m.group(3))
                task["download_speed"] = f"{m.group(4)}/s"
                watchdog.update_progress(parse_size_to_bytes(m.group(1)), parse_speed_to_bps(m.group(4)))
        proc.wait()
        if proc.returncode == 0 and os.path.exists(raw_path) and os.path.getsize(raw_path) > 1024:
            aria_success = True
    except Exception:
        pass
    finally:
        watchdog.stop()
        if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed") or (proc and proc.poll() is None):
            _kill_proc(proc)
        task["active_proc"] = None
        if proc and proc in task.get("active_procs", []):
            task["active_procs"].remove(proc)

    if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
        return

    if not aria_success:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        try:
            if requests is not None:
                with requests.get(url, headers=headers, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded = 0
                    t_prev = time.time()
                    bytes_prev = 0
                    with open(raw_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
                                return
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                t_now = time.time()
                                if t_now - t_prev >= 0.5:
                                    spd_bps = (downloaded - bytes_prev) / max(0.001, t_now - t_prev)
                                    speed_mb = spd_bps / (1024 * 1024)
                                    pct = (downloaded / total_size) * 100 if total_size > 0 else 0
                                    task["download_pct"] = pct
                                    task["download_speed"] = f"{speed_mb:.1f} MB/s"
                                    task["downloaded_str"] = f"{downloaded / (1024*1024):.1f} MB"
                                    task["total_str"] = f"{total_size / (1024*1024):.1f} MB" if total_size > 0 else "Unknown"
                                    watchdog.update_progress(downloaded, spd_bps)
                                    t_prev = t_now
                                    bytes_prev = downloaded
            else:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as r:
                    total_size = int(r.headers.get('Content-Length', 0))
                    downloaded = 0
                    t_prev = time.time()
                    bytes_prev = 0
                    with open(raw_path, 'wb') as f:
                        while True:
                            if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
                                return
                            chunk = r.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)
                            t_now = time.time()
                            if t_now - t_prev >= 0.5:
                                spd_bps = (downloaded - bytes_prev) / max(0.001, t_now - t_prev)
                                speed_mb = spd_bps / (1024 * 1024)
                                pct = (downloaded / total_size) * 100 if total_size > 0 else 0
                                task["download_pct"] = pct
                                task["download_speed"] = f"{speed_mb:.1f} MB/s"
                                task["downloaded_str"] = f"{downloaded / (1024*1024):.1f} MB"
                                task["total_str"] = f"{total_size / (1024*1024):.1f} MB" if total_size > 0 else "Unknown"
                                watchdog.update_progress(downloaded, spd_bps)
                                t_prev = t_now
                                bytes_prev = downloaded
        finally:
            watchdog.stop()
    else:
        watchdog.stop()

    task["download_pct"] = 100.0
    if os.path.exists(raw_path):
        task["raw_size_mb"] = os.path.getsize(raw_path) / (1024 * 1024)

def download_aria2c_torrent(magnet_url: str, task: Dict[str, Any]) -> str:
    """
    Direct BitTorrent downloader using aria2c with DHT, active public trackers,
    pre-fetched metadata cache, zero seeding limit, and selective file support.
    """
    raw_dir = os.path.join(LOCAL_RAW_DIR, f"task_{task['id']}_torrent")
    os.makedirs(raw_dir, exist_ok=True)

    trackers = [
        "udp://tracker.opentrackr.org:1337/announce",
        "udp://open.tracker.cl:1337/announce",
        "udp://9.rarbg.to:2920/announce",
        "udp://tracker.openbittorrent.com:6969/announce",
        "http://tracker.openbittorrent.com:80/announce",
        "udp://opentracker.i2p.rocks:6969/announce",
        "udp://tracker.torrent.eu.org:451/announce",
        "udp://open.stealth.si:80/announce",
        "udp://explodie.org:6969/announce",
        "udp://tracker.tiny-vps.com:6969/announce"
    ]

    f_idx = task.get("torrent_file_index") or task.get("target_file_index")
    target_torrent = magnet_url

    # Check 1: pre-cached metadata path in task
    if task.get("torrent_cached_path") and os.path.isfile(task["torrent_cached_path"]) and os.path.getsize(task["torrent_cached_path"]) > 100:
        target_torrent = task["torrent_cached_path"]
        task["download_speed"] = "Loading pre-cached metadata..."
    elif target_torrent.startswith("magnet:?"):
        m_hash = re.search(r'urn:btih:([a-fA-F0-9]{40}|[a-zA-Z2-7]{32})', target_torrent, re.I)
        btih = m_hash.group(1) if m_hash else ""
        meta_cache_dir = os.path.join(TEMP_DIR, "torrent_meta")
        os.makedirs(meta_cache_dir, exist_ok=True)

        found_torrent = None
        # Check cache directory for existing .torrent
        for cand_name in [f"{btih.upper()}.torrent", f"{btih.lower()}.torrent", "metadata.torrent"]:
            cand_p = os.path.join(meta_cache_dir, cand_name)
            if os.path.isfile(cand_p) and os.path.getsize(cand_p) > 100:
                found_torrent = cand_p
                break

        # Check online cache (itorrents.org)
        if not found_torrent and btih:
            try:
                creq = urllib.request.Request(f"https://itorrents.org/torrent/{btih.upper()}.torrent", headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(creq, timeout=3) as c_resp:
                    if c_resp.status == 200:
                        c_bytes = c_resp.read()
                        if len(c_bytes) > 200 and (b"d8:announce" in c_bytes or b"4:info" in c_bytes):
                            out_p = os.path.join(meta_cache_dir, f"{btih.lower()}.torrent")
                            with open(out_p, "wb") as cf:
                                cf.write(c_bytes)
                            found_torrent = out_p
            except Exception:
                pass

        # If selective downloading is requested on a magnet, aria2c requires a .torrent file
        if not found_torrent and f_idx:
            task["download_speed"] = "Resolving torrent metadata for selective file..."
            meta_cmd = [
                "aria2c",
                "--bt-metadata-only=true",
                "--bt-save-metadata=true",
                f"--dir={meta_cache_dir}",
                "--seed-time=0",
                "--enable-dht=true",
                "--dht-entry-point=router.bittorrent.com:6881",
                "--dht-entry-point6=dht.transmissionbt.com:6881",
                "--bt-enable-lpd=true",
                "--bt-max-peers=80",
                "--bt-tracker=" + ",".join(trackers),
                "--bt-stop-timeout=35",
                "--summary-interval=1",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                target_torrent
            ]
            popen_meta_kwargs = {"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "text": True, "bufsize": 1}
            if os.name != 'nt':
                popen_meta_kwargs["start_new_session"] = True
            m_proc = None
            try:
                m_proc = subprocess.Popen(meta_cmd, **popen_meta_kwargs)
                task["active_proc"] = m_proc
                task.setdefault("active_procs", []).append(m_proc)
                for m_line in m_proc.stdout:
                    if task.get("cancelled"):
                        _kill_proc(m_proc)
                        return ""
                    m_cn = re.search(r'CN:(\d+)', m_line)
                    peers_str = f" ({m_cn.group(1)} peers)" if m_cn else ""
                    task["download_speed"] = f"Fetching torrent metadata...{peers_str}"
                m_proc.wait()
            finally:
                if task.get("cancelled") or (m_proc and m_proc.poll() is None):
                    _kill_proc(m_proc)
                task["active_proc"] = None
                if m_proc and m_proc in task.get("active_procs", []):
                    task["active_procs"].remove(m_proc)

            if task.get("cancelled"):
                return ""

            # Check if aria2c created the .torrent file
            for cand_name in [f"{btih.lower()}.torrent", f"{btih.upper()}.torrent"]:
                cand_p = os.path.join(meta_cache_dir, cand_name)
                if os.path.isfile(cand_p) and os.path.getsize(cand_p) > 100:
                    found_torrent = cand_p
                    break

            if not found_torrent:
                for fn in os.listdir(meta_cache_dir):
                    if fn.endswith(".torrent"):
                        cand_p = os.path.join(meta_cache_dir, fn)
                        if os.path.getsize(cand_p) > 100:
                            found_torrent = cand_p
                            break

        if found_torrent:
            target_torrent = found_torrent
            task["torrent_cached_path"] = found_torrent

    if f_idx and target_torrent.startswith("magnet:?"):
        raise RuntimeError(f"Could not retrieve torrent metadata for selective file #{f_idx}. Magnet link may have no active seeders.")

    aria_cmd = [
        "aria2c",
        "--dir", raw_dir,
        "--seed-time=0",
        "--max-upload-limit=1K",
        "--enable-dht=true",
        "--dht-entry-point=router.bittorrent.com:6881",
        "--dht-entry-point6=dht.transmissionbt.com:6881",
        "--bt-enable-lpd=true",
        "--bt-max-peers=80",
        "--bt-tracker=" + ",".join(trackers),
        "--bt-require-crypto=false",
        "--bt-min-crypto-level=plain",
        "--file-allocation=none",
        "--summary-interval=1",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    ]

    if target_torrent.endswith(".torrent") or os.path.isfile(target_torrent):
        aria_cmd.append("--follow-torrent=mem")
    else:
        aria_cmd.append("--follow-torrent=mem")
        aria_cmd.append("--bt-save-metadata=true")

    if f_idx:
        aria_cmd.append(f"--select-file={f_idx}")
        aria_cmd.append("--bt-remove-unselected-file=true")

    aria_cmd.append(target_torrent)

    task["download_speed"] = "Connecting to swarm..."
    popen_kwargs = {"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "text": True, "bufsize": 1}
    if os.name != 'nt':
        popen_kwargs["start_new_session"] = True
    proc = None
    watchdog = DownloadWatchdog(task)
    watchdog.start()

    pat_progress = re.compile(r'([\d.]+[KMGT]i?B)/([\d.]+[KMGT]i?B)\s*\((\d+)%\).*?(?:CN:(\d+))?.*?DL:([\d.]+[KMGT]i?B)')
    pat_peers = re.compile(r'CN:(\d+)')
    pat_dl = re.compile(r'DL:([\d.]+[KMGT]i?B)')
    pat_meta = re.compile(r'(?:allocating|metadata|fetching)', re.I)

    try:
        proc = subprocess.Popen(aria_cmd, **popen_kwargs)
        task["active_proc"] = proc
        task.setdefault("active_procs", []).append(proc)
        for line in proc.stdout:
            if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
                _kill_proc(proc)
                return ""

            m = pat_progress.search(line)
            if m:
                task["downloaded_str"] = m.group(1)
                task["total_str"] = m.group(2)
                task["download_pct"] = float(m.group(3))
                peers_str = f" ({m.group(4)} peers)" if m.group(4) else ""
                task["download_speed"] = f"{m.group(5)}/s{peers_str}"
                watchdog.update_progress(parse_size_to_bytes(m.group(1)), parse_speed_to_bps(m.group(5)))
            else:
                cn_m = pat_peers.search(line)
                dl_m = pat_dl.search(line)
                if dl_m:
                    task["download_speed"] = f"{dl_m.group(1)}/s" + (f" ({cn_m.group(1)} peers)" if cn_m else "")
                    watchdog.update_progress(0, parse_speed_to_bps(dl_m.group(1)))
                elif pat_meta.search(line):
                    peers_str = f" ({cn_m.group(1)} peers)" if cn_m else ""
                    task["download_speed"] = f"Fetching metadata...{peers_str}"
        proc.wait()
    finally:
        watchdog.stop()
        if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed") or (proc and proc.poll() is None):
            _kill_proc(proc)
        task["active_proc"] = None
        if proc and proc in task.get("active_procs", []):
            task["active_procs"].remove(proc)

    if task.get("cancelled") or task.get("timed_out_start") or task.get("demote_low_speed"):
        return ""

    if proc.returncode != 0:
        raise RuntimeError(f"aria2c torrent download failed (exit code {proc.returncode}). Check if the magnet link has active seeds.")

    target_filename = task.get("torrent_filename")
    target_match = None
    largest_file = None
    largest_size = 0
    media_extensions = {'.mkv', '.mp4', '.avi', '.mov', '.ts', '.webm', '.flv', '.m4v', '.wmv', '.m2ts'}

    for root, _, files in os.walk(raw_dir):
        for f in files:
            if f.endswith(".aria2") or f.endswith(".torrent"):
                continue
            fp = os.path.join(root, f)
            sz = os.path.getsize(fp)
            ext = os.path.splitext(f)[1].lower()
            if target_filename and f.lower() == target_filename.lower() and sz > 1024 * 1024:
                target_match = fp
                largest_size = sz
                break
            if ext in media_extensions or sz > largest_size:
                if sz > largest_size:
                    largest_size = sz
                    largest_file = fp
        if target_match:
            largest_file = target_match
            break

    if not largest_file or largest_size < 1024 * 1024:
        raise RuntimeError(f"No valid media file found in downloaded torrent ({largest_size} bytes downloaded).")

    task["download_pct"] = 100.0
    task["raw_size_mb"] = largest_size / (1024 * 1024)
    return largest_file
