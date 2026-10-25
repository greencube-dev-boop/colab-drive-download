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
    add_markdown("""# 🎬 Colab Video Downloader & 480p Ultra-Compact MKV Batch Engine

An asynchronous batch video processing web application running natively in Google Colab with an interactive **Gradio** dashboard, hardware NVENC GPU encoding, and direct Google Drive preservation.

---

### 🌟 Key Architecture & Capabilities:
1. **Organized Tabbed Interface (Zero Clutter)**:
   - **Tab 1: 🔎 Instant Search & Episode Discovery**: Search any Movie or TV Show by title (e.g. `FBI`, `Inception`, `Breaking Bad`, `Reacher`). View posters, IMDb IDs, cast, official episode lists, and live multi-tracker torrent swarms.
   - **Tab 2: 🧲 Torrent & Magnet Explorer (Single Unified Hub)**: Dedicated single location for inspecting magnets and `.torrent` files. Filter by folder, choose custom episodes, and preserve folder hierarchy in Google Drive.
   - **Tab 3: 🔗 Direct Links & Custom Streams**: For direct MP4/MKV links, Videm/VidSrc embeds, M3U8 playlists, YouTube/yt-dlp links.
2. **Multi-Source Torrent Swarm Indexer (Integrated from en.yts.lu)**:
   - Real-time swarm search across The Pirate Bay, YTS, 1337x, and public trackers.
   - Quality tags (`[4K]`, `[1080p]`, `[720p]`, `[SD]`) with active seed/peer health counts.
   - 1-Click direct queue or 1-click bridge to the Torrent Explorer to cherry-pick specific episodes.
   - Auto-syncs swarms when switching TV seasons.
3. **Resilient Streaming & Embed Ingestion**:
   - Videm 5-server multi-probing with sliding-window HLS downloader (8 workers) and token refresh.
   - VidSrc / vsembed support with automated TMDb-to-IMDb translation and clean scene release naming.
4. **Working 1-Click Cancel on Every Card**:
   - Native FastAPI `/api/task/{id}/cancel` endpoint terminates ffmpeg/aria2c immediately and purges workspace files.
5. **Clean Processes Panel**:
   - Single `🧹 Clear Completed` button in header.
   - Live real-time visual progress tiles (download %, speed, ETA, parallel GPU chunk bars).
6. **Hardware Acceleration & Zero-Desync Audio**:
   - Tesla T4 GPU NVENC hardware encoding (`hevc_nvenc` with CRF 32).
   - Continuous Opus audio extraction (100% sync-safe, zero drift).
   - Saves directly to `/content/drive/MyDrive/vid/<torrent_name>/<folder>/`.
7. **Persistent Cell Keep-Alive Heartbeat**:
   - Runs a live runtime telemetry loop so Google Colab never enters idle mode.
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
try:
    gpu_info = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=name,memory.total,driver_version", "--format=csv,noheader"],
        text=True
    ).strip()
    print(f"✅ Active GPU Detected: {gpu_info}")
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
