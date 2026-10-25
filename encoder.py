import os
import re
import json
import glob
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List
from config import TEMP_DIR, DRIVE_DIR, sanitize_filename, tasks_lock

# Cache NVENC detection
_nvenc_checked = False
_gpu_nvenc_available = False
_nvenc_preset = "p4"

def is_nvenc_available() -> bool:
    """Verifies that NVIDIA GPU and hevc_nvenc are not just listed, but actually functional."""
    global _nvenc_checked, _gpu_nvenc_available, _nvenc_preset
    if not _nvenc_checked:
        _gpu_nvenc_available = False
        try:
            # 1. Quick check: is nvidia-smi responding?
            smi = subprocess.run(["nvidia-smi"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=4)
            if smi.returncode == 0:
                # 2. Check if ffmpeg lists hevc_nvenc
                res = subprocess.run(["ffmpeg", "-encoders"], capture_output=True, text=True, timeout=5)
                if "hevc_nvenc" in res.stdout:
                    # 3. Dry-run test encode of a tiny 0.1s synthetic black frame at 854x480
                    for preset in ["p4", "medium"]:
                        test_cmd = [
                            "ffmpeg", "-nostdin", "-y",
                            "-f", "lavfi", "-i", "color=c=black:s=854x480:d=0.1",
                            "-c:v", "hevc_nvenc",
                            "-preset", preset,
                            "-pix_fmt", "yuv420p",
                            "-f", "null", "-"
                        ]
                        dry_res = subprocess.run(test_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=8)
                        if dry_res.returncode == 0:
                            _gpu_nvenc_available = True
                            _nvenc_preset = preset
                            break
        except Exception:
            _gpu_nvenc_available = False
        _nvenc_checked = True
    return _gpu_nvenc_available

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

def convert_worker(task: Dict[str, Any]):
    """Transcodes downloaded raw video into an ultra-compact 480p MKV with Opus audio."""
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

    # 2. Target 480p calculation preserving aspect ratio
    if src_w >= src_h:
        target_h = min(src_h, 480)
        target_w = int(round((target_h * (src_w / src_h)) / 2.0) * 2)
    else:
        target_w = min(src_w, 480)
        target_h = int(round((target_w * (src_h / src_w)) / 2.0) * 2)
    target_w = target_w if target_w % 2 == 0 else target_w + 1
    target_h = target_h if target_h % 2 == 0 else target_h + 1

    scale_filter = f"scale={target_w}:{target_h}:flags=lanczos,setsar=1"

    # 3. Choose Video Codec args (NVENC Hardware GPU or libx265 CPU fallback)
    use_nvenc = is_nvenc_available()
    if use_nvenc:
        v_codec_args = [
            "-c:v", "hevc_nvenc",
            "-preset", _nvenc_preset,
            "-rc", "vbr",
            "-cq:v", "32",
            "-b:v", "0",
            "-maxrate", "550k",
            "-bufsize", "1200k",
            "-pix_fmt", "yuv420p"
        ]
    else:
        v_codec_args = [
            "-c:v", "libx265",
            "-crf", "32",
            "-preset", "veryfast",
            "-maxrate", "550k",
            "-bufsize", "1200k",
            "-pix_fmt", "yuv420p"
        ]

    cpu_fallback_args = [
        "-c:v", "libx265",
        "-crf", "32",
        "-preset", "veryfast",
        "-maxrate", "550k",
        "-bufsize", "1200k",
        "-pix_fmt", "yuv420p"
    ]

    if task.get("cancelled"):
        return

    # 4. Continuous Opus audio extraction (100% sync-safe, zero drift!)
    audio_path = os.path.join(TEMP_DIR, f"audio_{task['id']}.opus")
    if os.path.exists(audio_path):
        os.remove(audio_path)

    if a_streams:
        a_cmd = ["ffmpeg", "-nostdin", "-y", "-i", task["raw_path"], "-vn", "-c:a", "libopus", "-b:a", "64k", "-vbr", "on", audio_path]
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
        seg_target_duration = max(30, int(duration / num_workers))
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

    # 6. Parallel Encoding of Chunks with Auto CPU Fallback
    def encode_single_chunk(idx: int, chunk_path: str) -> str:
        base = os.path.splitext(os.path.basename(chunk_path))[0]
        out_chunk = os.path.join(task_chunks_dir, f"enc_{base}.mkv")
        log_file = os.path.join(task_chunks_dir, f"log_{base}.txt")
        task["chunk_status"][idx]["status"] = "Encoding"

        def run_ffmpeg(selected_args):
            cmd = [
                "ffmpeg", "-nostdin", "-y",
                "-i", chunk_path,
                "-vf", scale_filter,
                *selected_args,
                "-an",
                "-avoid_negative_ts", "make_zero",
                "-progress", "pipe:1",
                out_chunk
            ]
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
                            except ValueError:
                                pass
                        elif line.startswith("speed="):
                            task["chunk_status"][idx]["speed"] = line.split("=")[1].strip()
                        elif line == "progress=end":
                            task["chunk_status"][idx]["curr_sec"] = task["chunk_status"][idx]["dur_sec"]
                            task["chunk_status"][idx]["pct"] = 100.0
                            task["chunk_status"][idx]["status"] = "Done"
                            break
                    proc.wait()
                    return proc.returncode
                finally:
                    unregister_proc(task, proc)

        # Primary encode attempt (GPU NVENC if available, or CPU)
        code = run_ffmpeg(v_codec_args)

        if task.get("cancelled"):
            return out_chunk

        # Automatic CPU Fallback if NVENC failed during the run
        if code != 0 and use_nvenc:
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

    with ThreadPoolExecutor(max_workers=min(num_workers, len(raw_chunks))) as executor:
        futures = [executor.submit(encode_single_chunk, idx, cp) for idx, cp in enumerate(raw_chunks)]
        encoded_chunks = [f.result() for f in futures]

    if task.get("cancelled"):
        return

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

    stem = os.path.splitext(task["title"])[0]
    task["temp_path"] = os.path.join(TEMP_DIR, f"{stem}_480p.mkv")
    if os.path.exists(task["temp_path"]):
        os.remove(task["temp_path"])

    mux_cmd = ["ffmpeg", "-nostdin", "-y", "-i", stitched_video]
    if os.path.exists(audio_path):
        mux_cmd.extend(["-i", audio_path, "-c", "copy"])
    else:
        mux_cmd.extend(["-c", "copy"])
    mux_cmd.append(task["temp_path"])

    m_proc = subprocess.Popen(mux_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    register_proc(task, m_proc)
    try:
        m_proc.wait()
    finally:
        unregister_proc(task, m_proc)

    task["convert_pct"] = 100.0
    task["final_size_mb"] = os.path.getsize(task["temp_path"]) / (1024 * 1024)

    # Clean intermediate chunk directory
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
