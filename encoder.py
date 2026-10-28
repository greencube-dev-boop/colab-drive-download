import os
import re
import json
import glob
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List
from config import TEMP_DIR, DRIVE_DIR, sanitize_filename, tasks_lock

# Cache NVENC detections
_nvenc_checked = False
_gpu_hevc_nvenc_available = False
_gpu_h264_nvenc_available = False
_hevc_preset = "p4"
_h264_preset = "p4"

def check_hardware_encoders():
    """Verifies that NVIDIA GPU, hevc_nvenc, and h264_nvenc are actually functional."""
    global _nvenc_checked, _gpu_hevc_nvenc_available, _gpu_h264_nvenc_available, _hevc_preset, _h264_preset
    if not _nvenc_checked:
        _gpu_hevc_nvenc_available = False
        _gpu_h264_nvenc_available = False
        try:
            smi = subprocess.run(["nvidia-smi"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=4)
            if smi.returncode == 0:
                res = subprocess.run(["ffmpeg", "-encoders"], capture_output=True, text=True, timeout=5)
                encoders_str = res.stdout

                # 1. Test HEVC NVENC
                if "hevc_nvenc" in encoders_str:
                    for preset in ["p4", "medium"]:
                        test_cmd = [
                            "ffmpeg", "-nostdin", "-y",
                            "-f", "lavfi", "-i", "color=c=black:s=854x480:d=0.1",
                            "-c:v", "hevc_nvenc",
                            "-preset", preset,
                            "-pix_fmt", "yuv420p",
                            "-f", "null", "-"
                        ]
                        dry = subprocess.run(test_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=8)
                        if dry.returncode == 0:
                            _gpu_hevc_nvenc_available = True
                            _hevc_preset = preset
                            break

                # 2. Test H.264 NVENC
                if "h264_nvenc" in encoders_str:
                    for preset in ["p4", "medium"]:
                        test_cmd = [
                            "ffmpeg", "-nostdin", "-y",
                            "-f", "lavfi", "-i", "color=c=black:s=854x480:d=0.1",
                            "-c:v", "h264_nvenc",
                            "-preset", preset,
                            "-pix_fmt", "yuv420p",
                            "-f", "null", "-"
                        ]
                        dry = subprocess.run(test_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=8)
                        if dry.returncode == 0:
                            _gpu_h264_nvenc_available = True
                            _h264_preset = preset
                            break
        except Exception:
            pass
        _nvenc_checked = True

def is_nvenc_available() -> bool:
    check_hardware_encoders()
    return _gpu_hevc_nvenc_available

def is_h264_nvenc_available() -> bool:
    check_hardware_encoders()
    return _gpu_h264_nvenc_available

def register_proc(task: Dict[str, Any], proc: subprocess.Popen):
    """Registers a running subprocess on the task for instant cancellation."""
    with tasks_lock:
        task.setdefault("active_procs", []).append(proc)
        task["active_proc"] = proc

def unregister_proc(task: Dict[str, Any], proc: subprocess.Popen):
    """Removes a finished subprocess from the task's registry."""
    with tasks_lock:
        procs = task.get("active_procs", [])
        if proc in procs:
            procs.remove(proc)
        task["active_proc"] = procs[-1] if procs else None

def get_resolution_tag_and_limit(res_preset: str) -> tuple[str, int]:
    r = (res_preset or "").lower()
    if "240p" in r:
        return "240p", 240
    if "360p" in r:
        return "360p", 360
    if "720p" in r:
        return "720p", 720
    if "1080p" in r:
        return "1080p", 1080
    if "original" in r:
        return "orig", 99999
    return "480p", 480

def get_bitrate_for_resolution(res_limit: int) -> tuple[str, str]:
    if res_limit <= 240:
        return "350k", "700k"
    if res_limit <= 360:
        return "450k", "900k"
    if res_limit <= 480:
        return "600k", "1200k"
    if res_limit <= 720:
        return "1400k", "2800k"
    if res_limit <= 1080:
        return "2800k", "5600k"
    return "4000k", "8000k"

def convert_worker(task: Dict[str, Any]):
    """Transcodes downloaded raw video according to Universal Output Settings."""
    if task.get("cancelled"):
        return

    task_chunks_dir = os.path.join(TEMP_DIR, f"chunks_{task['id']}")
    os.makedirs(task_chunks_dir, exist_ok=True)

    # 1. Probe input video properties
    probe_cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", task["raw_path"]]
    probe_data = json.loads(subprocess.check_output(probe_cmd, text=True))
    v_streams = [s for s in probe_data.get("streams", []) if s.get("codec_type") == "video"]
    a_streams = [s for s in probe_data.get("streams", []) if s.get("codec_type") == "audio"]

    if not v_streams:
        raise ValueError("No video stream found in media file.")

    src_w = int(v_streams[0].get("width", 0))
    src_h = int(v_streams[0].get("height", 0))
    duration = float(probe_data.get("format", {}).get("duration", 0) or v_streams[0].get("duration", 0) or 1)
    task["duration"] = duration

    # 2. Target Resolution calculation preserving aspect ratio
    res_tag, res_limit = get_resolution_tag_and_limit(task.get("res_preset", "480p"))
    if res_limit >= 99999 or (src_w <= res_limit and src_h <= res_limit):
        target_w, target_h = src_w, src_h
        scale_filter = "null"
    else:
        if src_w >= src_h:
            target_h = min(src_h, res_limit)
            target_w = int(round((target_h * (src_w / src_h)) / 2.0) * 2)
        else:
            target_w = min(src_w, res_limit)
            target_h = int(round((target_w * (src_h / src_w)) / 2.0) * 2)
        target_w = target_w if target_w % 2 == 0 else target_w + 1
        target_h = target_h if target_h % 2 == 0 else target_h + 1
        scale_filter = f"scale={target_w}:{target_h}:flags=lanczos,setsar=1"

    max_v_rate, buf_v_rate = get_bitrate_for_resolution(res_limit)
    crf_val = int(task.get("crf_val", 32))

    # 3. Choose Video Codec args (NVENC Hardware GPU or CPU fallback)
    v_codec_choice = task.get("video_codec", "H.265 / HEVC")
    is_h264 = "264" in v_codec_choice or "AVC" in v_codec_choice

    if is_h264:
        use_gpu = is_h264_nvenc_available()
        if use_gpu:
            v_codec_args = [
                "-c:v", "h264_nvenc",
                "-preset", _h264_preset,
                "-rc", "vbr",
                "-cq:v", str(crf_val),
                "-b:v", "0",
                "-maxrate", max_v_rate,
                "-bufsize", buf_v_rate,
                "-pix_fmt", "yuv420p"
            ]
        else:
            v_codec_args = [
                "-c:v", "libx264",
                "-crf", str(crf_val),
                "-preset", "veryfast",
                "-maxrate", max_v_rate,
                "-bufsize", buf_v_rate,
                "-pix_fmt", "yuv420p"
            ]
        cpu_fallback_args = [
            "-c:v", "libx264",
            "-crf", str(crf_val),
            "-preset", "veryfast",
            "-maxrate", max_v_rate,
            "-bufsize", buf_v_rate,
            "-pix_fmt", "yuv420p"
        ]
    else:
        # H.265 / HEVC default
        use_gpu = is_nvenc_available()
        if use_gpu:
            v_codec_args = [
                "-c:v", "hevc_nvenc",
                "-preset", _hevc_preset,
                "-rc", "vbr",
                "-cq:v", str(crf_val),
                "-b:v", "0",
                "-maxrate", max_v_rate,
                "-bufsize", buf_v_rate,
                "-pix_fmt", "yuv420p"
            ]
        else:
            v_codec_args = [
                "-c:v", "libx265",
                "-crf", str(crf_val),
                "-preset", "veryfast",
                "-maxrate", max_v_rate,
                "-bufsize", buf_v_rate,
                "-pix_fmt", "yuv420p"
            ]
        cpu_fallback_args = [
            "-c:v", "libx265",
            "-crf", str(crf_val),
            "-preset", "veryfast",
            "-maxrate", max_v_rate,
            "-bufsize", buf_v_rate,
            "-pix_fmt", "yuv420p"
        ]

    if task.get("cancelled"):
        return

    # 4. Audio track extraction according to audio_preset
    audio_preset = task.get("audio_preset", "Opus 64 kbps")
    audio_path = os.path.join(TEMP_DIR, f"audio_{task['id']}")

    if "opus 48" in audio_preset.lower():
        audio_path += ".opus"
        a_args = ["-vn", "-c:a", "libopus", "-b:a", "48k", "-vbr", "on"]
    elif "aac 128" in audio_preset.lower():
        audio_path += ".aac"
        a_args = ["-vn", "-c:a", "aac", "-b:a", "128k"]
    elif "aac" in audio_preset.lower():
        audio_path += ".aac"
        a_args = ["-vn", "-c:a", "aac", "-b:a", "96k"]
    elif "copy" in audio_preset.lower() or "passthrough" in audio_preset.lower():
        audio_path += ".mka"
        a_args = ["-vn", "-c:a", "copy"]
    else:
        # Default: Opus 64 kbps (100% frame-perfect sync)
        audio_path += ".opus"
        a_args = ["-vn", "-c:a", "libopus", "-b:a", "64k", "-vbr", "on"]

    if os.path.exists(audio_path):
        os.remove(audio_path)

    if a_streams:
        a_cmd = ["ffmpeg", "-nostdin", "-y", "-i", task["raw_path"], *a_args, audio_path]
        a_proc = subprocess.Popen(a_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        register_proc(task, a_proc)
        try:
            a_proc.wait()
        finally:
            unregister_proc(task, a_proc)

    if task.get("cancelled"):
        return

    # 5. Keyframe-aligned video partitioning
    num_workers = task.get("parallel_workers", 3)
    raw_chunks: List[str] = []

    if duration > 90 and num_workers > 1:
        target_chunk_sec = int(getattr(config, "CHUNK_DURATION_SEC", 240.0))
        seg_target_duration = max(30, min(target_chunk_sec, int(duration / num_workers)))
        split_cmd = [
            "ffmpeg", "-nostdin", "-y",
            "-i", task["raw_path"],
            "-map", "0:v:0",
            "-c", "copy",
            "-f", "segment",
            "-segment_time", str(seg_target_duration),
            "-reset_timestamps", "1",
            os.path.join(task_chunks_dir, "seg_%03d.mkv")
        ]
        s_proc = subprocess.Popen(split_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        register_proc(task, s_proc)
        try:
            s_proc.wait()
        finally:
            unregister_proc(task, s_proc)

        if not task.get("cancelled"):
            raw_chunks = sorted(glob.glob(os.path.join(task_chunks_dir, "seg_*.mkv")))

    # Fallback to single chunk if splitting produced nothing or video was short
    if not raw_chunks:
        raw_chunks = [task["raw_path"]]

    task["chunk_status"] = {}
    for idx, cp in enumerate(raw_chunks):
        try:
            c_dur = float(subprocess.check_output([
                "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", cp
            ], text=True).strip())
        except Exception:
            c_dur = duration / len(raw_chunks)
        task["chunk_status"][idx] = {
            "curr_sec": 0.0,
            "dur_sec": max(c_dur, 1.0),
            "pct": 0.0,
            "speed": "--",
            "status": "Queued"
        }

    def _update_overall_progress():
        chunks = task.get("chunk_status", {})
        if chunks:
            total_curr = sum(c.get("curr_sec", 0.0) for c in chunks.values())
            total_dur = sum(c.get("dur_sec", 1.0) for c in chunks.values())
            if total_dur > 0:
                task["convert_pct"] = min(99.0, max(5.0, (total_curr / total_dur) * 100.0))

    # 6. Parallel Encoding of Chunks with Auto CPU Fallback
    def encode_single_chunk(idx: int, chunk_path: str) -> str:
        base = os.path.splitext(os.path.basename(chunk_path))[0]
        out_chunk = os.path.join(task_chunks_dir, f"enc_{base}.mkv")
        log_file = os.path.join(task_chunks_dir, f"log_{base}.txt")
        task["chunk_status"][idx]["status"] = "Encoding"

        def run_ffmpeg(selected_args):
            cmd = ["ffmpeg", "-nostdin", "-y", "-i", chunk_path]
            if scale_filter != "null":
                cmd.extend(["-vf", scale_filter])
            cmd.extend([
                *selected_args,
                "-an",
                "-avoid_negative_ts", "make_zero",
                "-progress", "pipe:1",
                out_chunk
            ])
            with open(log_file, "w") as lf:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=lf, text=True, bufsize=1)
                register_proc(task, proc)
                try:
                    for line in proc.stdout:
                        if task.get("cancelled"):
                            proc.terminate()
                            proc.kill()
                            return -1
                        line = line.strip()
                        if line.startswith("out_time_us="):
                            try:
                                us = int(line.split("=")[1])
                                curr_sec = us / 1_000_000
                                task["chunk_status"][idx]["curr_sec"] = curr_sec
                                dur_s = task["chunk_status"][idx]["dur_sec"]
                                task["chunk_status"][idx]["pct"] = min(100.0, (curr_sec / dur_s) * 100)
                                _update_overall_progress()
                            except ValueError:
                                pass
                        elif line.startswith("speed="):
                            task["chunk_status"][idx]["speed"] = line.split("=")[1].strip()
                        elif line == "progress=end":
                            task["chunk_status"][idx]["curr_sec"] = task["chunk_status"][idx]["dur_sec"]
                            task["chunk_status"][idx]["pct"] = 100.0
                            task["chunk_status"][idx]["status"] = "Done"
                            _update_overall_progress()
                            break
                    proc.wait()
                    return proc.returncode
                finally:
                    unregister_proc(task, proc)

        code = run_ffmpeg(v_codec_args)

        if task.get("cancelled"):
            return out_chunk

        # Automatic CPU Fallback if GPU hardware encoder failed during run
        if code != 0 and use_gpu:
            task["chunk_status"][idx]["status"] = "Retrying (CPU)"
            task["chunk_status"][idx]["speed"] = "--"
            code = run_ffmpeg(cpu_fallback_args)

        if task.get("cancelled"):
            return out_chunk

        if code != 0 or not os.path.exists(out_chunk):
            err_tail = ""
            if os.path.exists(log_file):
                try:
                    with open(log_file, "r") as lf:
                        lines = lf.readlines()
                        err_tail = " - " + "".join(lines[-4:]).strip()
                except Exception:
                    pass
            raise RuntimeError(f"Chunk encoding failed for {base}{err_tail}")

        task["chunk_status"][idx]["pct"] = 100.0
        task["chunk_status"][idx]["status"] = "Done"
        return out_chunk

    # Register job and chunks with ClusterManager for multi-GPU distributed conversion
    import cluster_manager

    chunks_dict = {}
    for idx, cp in enumerate(raw_chunks):
        base = os.path.splitext(os.path.basename(cp))[0]
        out_chunk_path = os.path.join(task_chunks_dir, f"enc_{base}.mkv")
        is_already_done = os.path.exists(out_chunk_path) and os.path.getsize(out_chunk_path) > 0
        chunks_dict[idx] = {
            "raw_path": cp,
            "out_path": out_chunk_path,
            "status": "done" if is_already_done else "pending",
            "worker_id": "cached" if is_already_done else None,
            "leased_until": 0.0,
            "pct": 100.0 if is_already_done else 0.0,
            "fps": 0.0,
            "speed": "--"
        }
        if is_already_done:
            task["chunk_status"][idx]["status"] = "Done"
            task["chunk_status"][idx]["pct"] = 100.0

    cluster_job_info = {
        "job_id": task["id"],
        "title": task.get("title", f"Job #{task['id']}"),
        "chunks_dir": task_chunks_dir,
        "raw_chunks": raw_chunks,
        "chunks": chunks_dict,
        "refining_config": {
            "res_preset": task.get("res_preset", "480p"),
            "scale_filter": scale_filter,
            "video_codec": task.get("video_codec", "H.265 / HEVC"),
            "crf_val": crf_val
        },
        "cancelled": False
    }
    cluster_manager.register_cluster_job(task["id"], cluster_job_info)

    def local_cluster_worker(local_worker_id: str):
        while not task.get("cancelled"):
            c_task = cluster_manager.claim_task(local_worker_id)
            if not c_task or not c_task.get("has_task"):
                if all(c.get("status") == "done" for c in chunks_dict.values()):
                    break
                time.sleep(0.5)
                continue

            c_idx = c_task["chunk_idx"]
            raw_cp = c_task["raw_chunk_path"]
            out_cp = c_task["out_chunk_path"]

            try:
                encode_single_chunk(c_idx, raw_cp)
                fsize = os.path.getsize(out_cp) / (1024 * 1024) if os.path.exists(out_cp) else 0.0
                cluster_manager.complete_task(local_worker_id, task["id"], c_idx, out_cp, fsize)
            except Exception as e:
                print(f"[Encoder] Local chunk #{c_idx} encoding notice: {e}")
                chunks_dict[c_idx]["status"] = "pending"
                chunks_dict[c_idx]["worker_id"] = None
                time.sleep(1.0)

    # Launch local worker threads alongside any remote cluster helpers
    local_concurrency = min(num_workers, len(raw_chunks))
    with ThreadPoolExecutor(max_workers=local_concurrency) as executor:
        futures = [executor.submit(local_cluster_worker, f"main_local_{i}") for i in range(local_concurrency)]
        while not task.get("cancelled"):
            all_done = all(c.get("status") == "done" for c in chunks_dict.values())
            # Sync remote helper telemetry into task["chunk_status"]
            for idx, c in chunks_dict.items():
                if c.get("status") == "done":
                    task["chunk_status"][idx]["status"] = "Done"
                    task["chunk_status"][idx]["pct"] = 100.0
                elif c.get("status") == "leased":
                    task["chunk_status"][idx]["status"] = "Encoding"
                    if c.get("pct", 0) > 0:
                        task["chunk_status"][idx]["pct"] = c["pct"]
                    if c.get("speed"):
                        task["chunk_status"][idx]["speed"] = c["speed"]
            _update_overall_progress()
            if all_done:
                break
            time.sleep(0.5)

        for f in futures:
            try:
                f.result(timeout=1.0)
            except Exception:
                pass

    cluster_manager.unregister_cluster_job(task["id"])

    if task.get("cancelled"):
        return

    encoded_chunks = [c["out_path"] for c in sorted(chunks_dict.values(), key=lambda x: x["raw_path"])]

    # 7. Concat video chunks and mux with continuous audio
    encoded_chunks.sort()
    concat_list_file = os.path.join(TEMP_DIR, f"concat_{task['id']}.txt")
    with open(concat_list_file, "w") as lf:
        for c in encoded_chunks:
            escaped_path = c.replace("\\", "/")
            lf.write(f"file '{escaped_path}'\n")

    stitched_video = os.path.join(TEMP_DIR, f"stitched_{task['id']}.mkv")
    c_proc = subprocess.Popen([
        "ffmpeg", "-nostdin", "-y",
        "-f", "concat", "-safe", "0",
        "-i", concat_list_file,
        "-c", "copy",
        stitched_video
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    register_proc(task, c_proc)
    try:
        c_proc.wait()
    finally:
        unregister_proc(task, c_proc)

    if task.get("cancelled"):
        return

    # 8. Mux into chosen container (MKV or MP4)
    container_choice = task.get("container_format", "mkv").lower()
    target_ext = ".mp4" if "mp4" in container_choice else ".mkv"

    stem = os.path.splitext(task["title"])[0]
    # Remove existing resolution tag if user provided it to avoid double tags
    stem = re.sub(r'_(240p|360p|480p|720p|1080p|orig)$', '', stem, flags=re.I)
    task["temp_path"] = os.path.join(TEMP_DIR, f"{stem}_{res_tag}{target_ext}")
    if os.path.exists(task["temp_path"]):
        os.remove(task["temp_path"])

    mux_cmd = ["ffmpeg", "-nostdin", "-y", "-i", stitched_video]
    if os.path.exists(audio_path):
        mux_cmd.extend(["-i", audio_path, "-c", "copy"])
    else:
        mux_cmd.extend(["-c", "copy"])

    if target_ext == ".mp4":
        mux_cmd.extend(["-movflags", "+faststart"])

    mux_cmd.append(task["temp_path"])

    m_proc = subprocess.Popen(mux_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    register_proc(task, m_proc)
    try:
        m_proc.wait()
    finally:
        unregister_proc(task, m_proc)

    task["convert_pct"] = 100.0
    task["final_size_mb"] = os.path.getsize(task["temp_path"]) / (1024 * 1024)

    # Clean intermediate chunk directory and files
    shutil.rmtree(task_chunks_dir, ignore_errors=True)
    for p in [stitched_video, concat_list_file, audio_path]:
        if os.path.exists(p):
            os.remove(p)

def move_to_drive_worker(task: Dict[str, Any]):
    """Preserves folder hierarchy in Google Drive and cleans workspace."""
    if task.get("cancelled"):
        return

    drive_dest_dir = DRIVE_DIR
    if task.get("torrent_name"):
        drive_dest_dir = os.path.join(drive_dest_dir, sanitize_filename(task["torrent_name"]))
        if task.get("rel_folder") and task["rel_folder"] != "Root":
            folder_parts = [sanitize_filename(p) for p in task["rel_folder"].replace('\\', '/').split('/') if p]
            if folder_parts:
                drive_dest_dir = os.path.join(drive_dest_dir, *folder_parts)

    os.makedirs(drive_dest_dir, exist_ok=True)

    base_name = os.path.basename(task["temp_path"])
    final_dest = os.path.join(drive_dest_dir, base_name)
    stem, ext = os.path.splitext(base_name)
    c = 1
    while os.path.exists(final_dest):
        final_dest = os.path.join(drive_dest_dir, f"{stem}_{c}{ext}")
        c += 1

    shutil.copy2(task["temp_path"], final_dest)
    task["drive_path"] = final_dest

    # Workspace cleanup
    if os.path.exists(task.get("temp_path", "")):
        try:
            os.remove(task["temp_path"])
        except Exception:
            pass
    if os.path.exists(task.get("raw_path", "")):
        try:
            os.remove(task["raw_path"])
        except Exception:
            pass
