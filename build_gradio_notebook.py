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
        "logger.py",
        "database.py",
        "metadata_manager.py",
        "classifier.py",
        "torrent_parser.py",
        "torrent_search.py",
        "media_search.py",
        "resolvers.py",
        "downloader.py",
        "encoder.py",
        "pipeline.py",
        "tunnel.py",
        "cluster_manager.py",
        "helper_worker.py",
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
    add_markdown("""# 🚀 Two-Stage Google Drive Downloader & GPU Refiner

An enterprise-grade, asynchronous batch media ingestion and transcoding system running natively in Google Colab with an interactive **Gradio** dashboard, persistent SQLite queue, Gemini AI media classification, hardware NVENC GPU encoding, and direct Google Drive preservation.

---

### 🌟 Key Architecture & Capabilities:
1. **🌐 Universal Settings Bar (Top Header)**:
   - **Ingestion Settings**: Configurable connection / stream concurrency (1–16 connections) and cloud Debrid support.
   - **Refining Settings**: `480p`, `240p`, `360p`, `720p`, `1080p`, or `Original`.
   - **Video Codecs**: `H.265 / HEVC` or `H.264 / AVC` with GPU NVENC hardware acceleration and automatic CPU fallback.
   - **Container Formats**: `MKV` or `MP4` (`+faststart`).
   - **Audio Presets**: `Opus 64 kbps` (zero-desync frame-perfect), `Opus 48 kbps`, `AAC 96 kbps`, `AAC 128 kbps`, or `Copy Original`.
   - **GPU VRAM Slices & CRF**: Slicing slider (1–8 chunks) and custom CRF compression slider (18–36).
2. **Two-Stage Decoupled Workflow**:
   - **Stage 1 (Ingestion)**: Downloads video to Colab local workspace $\\rightarrow$ Classifies with AI (Gemini / Heuristic) $\\rightarrow$ Standardizes title (e.g. `Avatar Fire And Ash (2025)` or `Mentalist S01E04`) $\\rightarrow$ Moves raw video to `drive/downloader/raw/` $\\rightarrow$ Records entry in `metadata.json` $\\rightarrow$ Enqueues into persistent SQLite queue (`drive/downloader/data/queue.db`).
   - **Stage 2 (Refining)**: Background worker daemon continuously monitors `queue.db` $\\rightarrow$ Copies raw file from Drive to local VM $\\rightarrow$ Slices and encodes via NVENC in GPU VRAM $\\rightarrow$ Stitches and muxes final video $\\rightarrow$ Moves to `drive/downloader/refined/<tv_series|movies|other>` $\\rightarrow$ Verifies file on Drive $\\rightarrow$ **Bypasses Drive Trash Bin** (moves raw file to VM disk, then unlinks locally) $\\rightarrow$ Updates `metadata.json` & `queue.db`.
3. **Persistent SQLite Queue & Interactive Process Manager**:
   - If a Colab session ends or restarts, pending refining jobs in `queue.db` automatically resume in the next session without re-downloading!
   - 1-click **Move Up (⬆️)** and **Move Down (⬇️)** to reorder pending jobs.
   - Active cancellation and single **Clear Completed** button.
   - **Drive Media Catalog** tab provides a real-time browser for `metadata.json`.
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

print("=" * 72)
print("🚀 [1/3] Checking Hardware Acceleration (NVIDIA GPU)...")
print("=" * 72)
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
print("=" * 72)
try:
    from google.colab import drive
    drive.mount('/content/drive')
    print("✅ Google Drive mounted successfully at /content/drive")
except Exception as e:
    print(f"ℹ️ Google Drive mount skipped or already mounted: {e}")

print()
print("📦 [3/3] Installing Dependencies & Setting Up Project Workspace...")
print("=" * 72)
!apt-get update -qq && apt-get install -y -qq aria2 ffmpeg > /dev/null
!pip install -q gradio yt-dlp fastapi uvicorn requests urllib3 google-genai
!wget -q -nc https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O /usr/local/bin/cloudflared && chmod +x /usr/local/bin/cloudflared

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

print("📦 Deploying project modules into workspace...")
os.makedirs(REPO_DIR, exist_ok=True)
bundle_data = """ + repr(compressed_bundle) + """
decompressed = zlib.decompress(base64.b64decode(bundle_data)).decode('utf-8')
files_dict = json.loads(decompressed)
for fname, fcontent in files_dict.items():
    fpath = os.path.join(REPO_DIR, fname)
    with open(fpath, "w", encoding="utf-8") as f:
        f.write(fcontent)
print(f"✅ Deployed {len(files_dict)} modular project files successfully into {REPO_DIR}!")

try:
    if not os.path.exists(os.path.join(REPO_DIR, ".git")):
        subprocess.run(["git", "clone", "--depth", "1", REPO_URL, REPO_DIR + "_git_temp"], capture_output=True, text=True, timeout=15)
        if os.path.exists(REPO_DIR + "_git_temp/.git"):
            shutil.move(REPO_DIR + "_git_temp/.git", os.path.join(REPO_DIR, ".git"))
            shutil.rmtree(REPO_DIR + "_git_temp", ignore_errors=True)
except Exception:
    pass

os.chdir(REPO_DIR)
print(f"🎉 Setup complete! Current workspace: {os.getcwd()}")
""")

    # Cell 2: Launch Web App & Keep-Alive Loop
    add_code("""#@title 🚀 Step 2: Launch Web Dashboard & Keep-Alive Heartbeat
#@markdown Starts the web server with multi-path access: Cloudflare Tunnel, Google Colab native proxy, and Gradio share.
enable_debug_logs = True #@param {type:"boolean"}

import os
import sys

# Ensure workspace directory is in python search path
if os.path.exists("/content/colab-drive-download"):
    os.chdir("/content/colab-drive-download")
if "." not in sys.path:
    sys.path.insert(0, ".")

import logger
logger.set_debug(enable_debug_logs)

# Setup Colab native direct link window anchor
try:
    from google.colab import output
    output.serve_kernel_port_as_window(7860, anchor_text="🔗 [Colab Native Direct Link] Click here to open Web Dashboard in a new tab")
except Exception:
    pass

import app
app.main(enable_debug_logs=enable_debug_logs)
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

    NOTEBOOK_NAME = "drive_downloader_gpu_refiner.ipynb"
    with open(NOTEBOOK_NAME, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1)
    print(f"Clean, modular notebook successfully generated: {NOTEBOOK_NAME}")
