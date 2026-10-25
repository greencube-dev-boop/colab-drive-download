import os
import json
import ast
import base64
import zlib

def generate_notebook():
    cells = []

    def add_markdown(source):
        cells.append({
            "cell_type": "markdown",
            "metadata": {},
            "source": [line + "\n" for line in source.strip().split("\n")]
        })

    def add_code(source):
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [line + "\n" for line in source.strip().split("\n")]
        })

    # Read all modular files to bundle as fallback
    modules = [
        "config.py",
        "torrent_parser.py",
        "torrent_search.py",
        "media_search.py",
        "resolvers.py",
        "downloader.py",
        "encoder.py",
        "pipeline.py",
        "ui.py",
        "app.py",
        "requirements.txt"
    ]
    bundle = {}
    for mod in modules:
        if os.path.exists(mod):
            with open(mod, "r", encoding="utf-8") as f:
                bundle[mod] = f.read()

    bundle_json = json.dumps(bundle)
    compressed_bundle = base64.b64encode(zlib.compress(bundle_json.encode('utf-8'))).decode('ascii')

    # Cell 0: Modern Documentation
    add_markdown("""# 🎬 Colab Video Downloader & Universal Transcode Engine

An asynchronous batch video processing web application running natively in Google Colab with an interactive **Gradio** dashboard, hardware NVENC GPU encoding, and direct Google Drive preservation.

---

### 🌟 Key Architecture & Capabilities:
1. **⚙️ Universal Settings Bar (Persistent Header)**:
   - **Download Settings**: Configurable connection / segment concurrency (1–16 connections) and optional cloud Debrid bridges (TorBox / Real-Debrid).
   - **Output Resolution**: `480p (Default)`, `360p (Mobile Ultra-Low)`, `240p (Data Saver)`, `720p (HD)`, `1080p (Full HD)`, or `Original / Passthrough`.
   - **Video Codecs**: `H.265 / HEVC` (high compression) or `H.264 / AVC` (broad device playback) with GPU NVENC hardware acceleration and transparent CPU fallback.
   - **Container Formats**: `MKV` (multi-track resilient) or `MP4` (`+faststart` streaming).
   - **Audio Presets**: `Opus 64 kbps` (zero-desync frame-perfect), `Opus 48 kbps`, `AAC 96 kbps`, `AAC 128 kbps`, or `Copy Original`.
   - **Transcode Chunks & Quality**: Selectable parallel transcode slices (1–6 workers) and custom CRF compression slider (18–40).
2. **Clean 2-Tab Layout with Automatic Switch**:
   - **Main Tab 1: 📥 Fetch & Queue**:
     - *Sub-Tab 1: 🔗 Direct Download*: High-speed multi-connection downloads for direct video URLs, Videm/VidSrc embeds, and HLS M3U8 playlists.
     - *Sub-Tab 2: 🧲 Torrent & Magnet Download*: Dedicated hub to inspect `.torrent` files or magnet links, explore the complete directory hierarchy, and cherry-pick specific episodes.
     - *Sub-Tab 3: 🔎 Title Search & Discovery*: Instant IMDb search, TV series season/episode browser, and live multi-tracker torrent swarms (The Pirate Bay & YTS).
   - **Main Tab 2: 📊 Downloads & Processes**:
     - Automatically switches here whenever a download is queued!
     - Displays real-time progress, speed, ETA, parallel GPU chunk bars, and compression savings %.
     - Working **`✕ Cancel`** (active) and **`✕ Clear`** (errored/done) buttons that immediately kill subprocesses, delete temp files, and remove cards.
     - Single **`🧹 Clear Completed`** button for one-click cleanup.
""")

    # Cell 1: Environment Setup, Dependencies & Drive Mount
    add_code("""#@title 🛠️ Step 1: Environment Setup, Google Drive Mount & Project Sync
#@markdown Mounts Google Drive, checks Tesla T4 GPU hardware acceleration, installs system packages (`aria2`, `ffmpeg`), and prepares the workspace.

import os
import sys
import shutil
import subprocess
import json
import base64
import zlib

print("=" * 68)
print("🚀 [1/3] Checking Hardware Acceleration (NVIDIA GPU)...")
print("=" * 68)
has_gpu = False
try:
    gpu_info = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=name,memory.total,driver_version", "--format=csv,noheader"],
        text=True
    ).strip()
    print(f"✅ Active GPU Detected: {gpu_info}")
    has_gpu = True
except Exception:
    print("⚠️ No NVIDIA GPU detected. CPU encoding fallback will be used.")

print()
print("📁 [2/3] Mounting Google Drive...")
print("=" * 68)
try:
    from google.colab import drive
    drive.mount('/content/drive')
    print("✅ Google Drive mounted successfully at /content/drive")
except Exception as e:
    print(f"ℹ️ Google Drive mount skipped or already mounted: {e}")

print()
print("📦 [3/3] Installing Dependencies & Setting Up Project Workspace...")
print("=" * 68)
!apt-get update -qq && apt-get install -y -qq aria2 ffmpeg > /dev/null
!pip install -q gradio yt-dlp fastapi uvicorn requests urllib3

if has_gpu:
    try:
        test_nv = subprocess.run(
            ["ffmpeg", "-nostdin", "-y", "-f", "lavfi", "-i", "color=c=black:s=854x480:d=0.1", "-c:v", "hevc_nvenc", "-f", "null", "-"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5
        )
        if test_nv.returncode != 0:
            print("⚡ Fetching static CUDA/NVENC-enabled FFmpeg build for Tesla T4 acceleration...")
            fetch_script = (
                "wget -q -O /tmp/ffmpeg.tar.xz https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz && "
                "tar -xf /tmp/ffmpeg.tar.xz -C /tmp/ && "
                "cp /tmp/ffmpeg-master-latest-linux64-gpl/bin/ffmpeg /usr/local/bin/ffmpeg && "
                "cp /tmp/ffmpeg-master-latest-linux64-gpl/bin/ffprobe /usr/local/bin/ffprobe && "
                "chmod +x /usr/local/bin/ffmpeg /usr/local/bin/ffprobe && "
                "rm -rf /tmp/ffmpeg.tar.xz /tmp/ffmpeg-master*"
            )
            subprocess.run(fetch_script, shell=True, executable="/bin/bash")
            print("✅ High-performance CUDA/NVENC FFmpeg installed!")
        else:
            print("✅ System FFmpeg supports NVENC hardware acceleration.")
    except Exception as e:
        print(f"ℹ️ FFmpeg NVENC check notice: {e}")

REPO_URL = "https://github.com/greencube-dev-boop/colab-drive-download.git"
REPO_DIR = "/content/colab-drive-download"

git_synced = False
if not os.path.exists(REPO_DIR):
    clone_res = subprocess.run(["git", "clone", REPO_URL, REPO_DIR], capture_output=True, text=True)
    if clone_res.returncode == 0:
        git_synced = True
        print(f"✅ Cloned latest project from GitHub into {REPO_DIR}!")
else:
    pull_res = subprocess.run(["git", "-C", REPO_DIR, "pull"], capture_output=True, text=True)
    if pull_res.returncode == 0:
        git_synced = True
        print(f"✅ Pulled latest project updates from GitHub into {REPO_DIR}!")

if not git_synced:
    print("📦 Extracting embedded project modules into workspace...")
    os.makedirs(REPO_DIR, exist_ok=True)
    bundle_data = """ + repr(compressed_bundle) + """
    decompressed = zlib.decompress(base64.b64decode(bundle_data)).decode('utf-8')
    files_dict = json.loads(decompressed)
    for fname, fcontent in files_dict.items():
        fpath = os.path.join(REPO_DIR, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(fcontent)
    print(f"✅ Extracted {len(files_dict)} modular project files successfully into {REPO_DIR}!")

os.chdir(REPO_DIR)
print(f"🎉 Setup complete! Current workspace: {os.getcwd()}")
""")

    # Cell 2: Launch Web App & Keep-Alive Loop
    add_code("""#@title 🚀 Step 2: Launch Gradio Web Application & Heartbeat Keep-Alive
#@markdown Starts the Gradio web server with public `share=True` link and runs continuous telemetry heartbeat.

import os
import sys

# Ensure workspace directory is in python search path
if os.path.exists("/content/colab-drive-download"):
    os.chdir("/content/colab-drive-download")
if "." not in sys.path:
    sys.path.insert(0, ".")

!python app.py --share
""")

    nb = {
        "cells": cells,
        "metadata": {
            "accelerator": "GPU",
            "colab": {
                "provenance": []
            },
            "kernelspec": {
                "display_name": "Python 3",
                "name": "python3"
            },
            "language_info": {
                "name": "python"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 0
    }
    return nb

if __name__ == "__main__":
    nb = generate_notebook()
    # Validate Python AST of each code cell
    for i, cell in enumerate(nb["cells"]):
        if cell["cell_type"] == "code":
            src = "".join(cell["source"])
            clean_lines = [l for l in src.splitlines() if not l.startswith("!") and not l.startswith("#@") and not l.startswith("%")]
            try:
                ast.parse("\n".join(clean_lines))
                print(f"[OK] Cell {i} syntax is valid Python AST.")
            except SyntaxError as e:
                print(f"[FAIL] Cell {i} syntax error: {e}")
                raise

    with open("video_to_480p_mkv_drive.ipynb", "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1)
    print("Clean, modular notebook successfully generated: video_to_480p_mkv_drive.ipynb")
