import os
import re
import json
import glob
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List
from config import TEMP_DIR, DRIVE_DIR, sanitize_filename

# Cache NVENC detection
_nvenc_checked = False
_gpu_nvenc_available = False

def is_nvenc_available() -> bool:
    global _nvenc_checked, _gpu_nvenc_available
    if not _nvenc_checked:
        try:
            res = subprocess.run(["ffmpeg", "-encoders"], capture_output=True, text=True, timeout=5)
            _gpu_nvenc_available = "hevc_nvenc" in res.stdout
        except Exception:
            _gpu_nvenc_available = False
        _nvenc_checked = True
    return _gpu_nvenc_available

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
    if is_nvenc_available():
        v_codec_args = [
            "-c:v", "hevc_nvenc",
            "-preset", "p4",
            "-rc", "vbr",
            "-cq:v", "32",
            "-b:v", "0",
            "-maxrate", "550k",
            "-bufsize", "1200k",
            "-pix_fmt", "yuv420p"
        ]
        hw_args = ["-hwaccel", "cuda"]
    else:
        v_codec_args = [
            "-c:v", "libx265",
            "-crf", "32",
            "-preset", "veryfast",
            "-maxrate", "550k",
            "-bufsize", "1200k",
            "-pix_fmt", "yuv420p"
        ]
        hw_args = []

    if task.get("cancelled"):
        return

    # 4. Continuous Opus audio extraction (100% sync-safe, zero drift!)
    audio_path = os.path.join(TEMP_DIR, f"audio_{task['id']}.opus")
    if os.path.exists(audio_path):
        os.remove(audio_path)

    if a_streams:
        a_cmd = ["ffmpeg", "-nostdin", "-y", "-i", task["raw_path"], "-vn", "-c:a", "libopus", "-b:a", "64k", "-vbr", "on", audio_path]
        subprocess.run(a_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

    if task.get("cancelled"):
        return

    # 5. Keyframe-aligned video partitioning
    num_workers = task.get("parallel_workers", 3)
    seg_target_duration = max(30, int(duration / num_workers))
    split_cmd = [
        "ffmpeg", "-nostdin", "-y",
        "-i", task["raw_path"],
        "-map", "0:v:0",
        "-c", "copy",
        "-f", "segment",
        "-segment_time", str(seg_target_duration),
        "-reset_timestamps", "1",
        os.path.join(task_chunks_dir, "seg_%03d.mp4")
    ]
    subprocess.run(split_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    raw_chunks = sorted(glob.glob(os.path.join(task_chunks_dir, "seg_*.mp4")))

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

    # 6. Parallel Encoding of Chunks
    def encode_single_chunk(idx: int, chunk_path: str) -> str:
        base = os.path.splitext(os.path.basename(chunk_path))[0]
        out_chunk = os.path.join(task_chunks_dir, f"enc_{base}.mkv")
        log_file = os.path.join(task_chunks_dir, f"log_{base}.txt")
        task["chunk_status"][idx]["status"] = "Encoding"

        cmd = [
            "ffmpeg", "-nostdin", "-y",
            *hw_args,
            "-i", chunk_path,
            "-vf", scale_filter,
            *v_codec_args,
            "-an",
            "-avoid_negative_ts", "make_zero",
            "-progress", "pipe:1",
            out_chunk
        ]
        with open(log_file, "w") as lf:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=lf, text=True, bufsize=1)
            task["active_proc"] = proc
            try:
                for line in proc.stdout:
                    if task.get("cancelled"):
                        proc.terminate()
                        proc.kill()
                        return out_chunk
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
            finally:
                task["active_proc"] = None

        if task.get("cancelled"):
            return out_chunk

        if proc.returncode != 0 or not os.path.exists(out_chunk):
            raise RuntimeError(f"Chunk encoding failed for {base}")
        task["chunk_status"][idx]["pct"] = 100.0
        task["chunk_status"][idx]["status"] = "Done"
        return out_chunk

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(encode_single_chunk, idx, cp) for idx, cp in enumerate(raw_chunks)]
        encoded_chunks = [f.result() for f in futures]

    if task.get("cancelled"):
        return

    # 7. Concat video chunks and mux with continuous audio
    encoded_chunks.sort()
    concat_list_file = os.path.join(TEMP_DIR, f"concat_{task['id']}.txt")
    with open(concat_list_file, "w") as lf:
        for c in encoded_chunks:
            lf.write(f"file '{c}'\n")

    stitched_video = os.path.join(TEMP_DIR, f"stitched_{task['id']}.mkv")
    subprocess.run([
        "ffmpeg", "-nostdin", "-y",
        "-f", "concat", "-safe", "0",
        "-i", concat_list_file,
        "-c", "copy",
        stitched_video
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

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
    subprocess.run(mux_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

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
