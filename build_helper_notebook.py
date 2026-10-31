import os
import json
import base64
import zlib

def generate_helper_notebook():
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

    # Read modular files needed by the helper
    modules = [
        "config.py",
        "logger.py",
        "helper_worker.py"
    ]
    bundle = {}
    for mod in modules:
        if os.path.exists(mod):
            with open(mod, "r", encoding="utf-8") as f:
                bundle[mod] = f.read()

    bundle_json = json.dumps(bundle)
    compressed_bundle = base64.b64encode(zlib.compress(bundle_json.encode('utf-8'))).decode('ascii')

    # Cell 0: Documentation
    add_markdown("""# ⚡ Google Drive GPU Refiner - Remote Worker Node (Helper)

This lightweight helper notebook provides external GPU compute (NVIDIA Tesla T4 / V100 / A100 / L4) to accelerate video refining for the **Main Google Drive Downloader & GPU Refiner**.

---

### 🌟 How it Works:
1. **Shared Drive Integration**: Mounts the shared `downloader` Google Drive folder.
2. **Zero Configuration**: Automatically discovers the Main coordinator via `data/cluster/master.json` on Google Drive.
3. **Hardware NVENC Acceleration**: Receives 3–5 minute video chunks, transcodes them at maximum GPU speed using NVENC hardware acceleration, and saves the finished segments directly back into Google Drive.
4. **Fault Tolerant**: If this Colab runtime stops or is preempted, the Main coordinator automatically reclaims the chunk and reassigns it to another worker without losing any completed work.
""")

    # Cell 1: Mount Drive & Environment Setup
    add_code(f"""#@title 🛠️ Step 1: Environment Setup & Google Drive Mount
#@markdown Mounts Google Drive, verifies NVIDIA GPU hardware acceleration, and prepares workspace.

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
    print(f"✅ Active GPU Detected: {{gpu_info}}")
    has_gpu = True
except Exception:
    print("⚠️ No NVIDIA GPU detected! Helper will run in CPU fallback mode (slower).")
    print("👉 Recommendation: Click Runtime > Change runtime type > Select T4 GPU.")

print("\\n" + "=" * 72)
print("📁 [2/3] Mounting Google Drive...")
print("=" * 72)
try:
    from google.colab import drive
    drive.mount('/content/drive')
    print("✅ Google Drive mounted successfully at /content/drive")
except Exception as e:
    print(f"⚠️ Drive mount notice: {{e}}")

print("\\n" + "=" * 72)
print("📦 [3/3] Deploying Helper Modules...")
print("=" * 72)
workspace_dir = "/content/colab-gpu-helper"
os.makedirs(workspace_dir, exist_ok=True)
os.chdir(workspace_dir)
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

# Decompress and write modular files
bundle_b64 = "{compressed_bundle}"
try:
    raw_json = zlib.decompress(base64.b64decode(bundle_b64.encode('ascii'))).decode('utf-8')
    files_dict = json.loads(raw_json)
    for fname, content in files_dict.items():
        fpath = os.path.join(workspace_dir, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content)
    print(f"✅ Deployed {{len(files_dict)}} helper modules into {{workspace_dir}}")
except Exception as e:
    print(f"⚠️ Notice deploying bundle: {{e}}")

print("🎉 Setup complete! Helper workspace ready.")
""")

    # Cell 2: Launch Helper Worker
    add_code("""#@title 🚀 Step 2: Start Autonomous GPU Worker Loop
#@markdown Launches the helper node. It auto-discovers the Main coordinator and begins encoding video chunks.
coordinator_url = "" #@param {type:"string"}
drive_folder_name = "" #@param {type:"string"}
#@markdown *Optional*: If Google Drive auto-discovery takes time, paste the Cloudflare or Gradio link from the Main coordinator here (e.g. `https://xyz.trycloudflare.com`), or specify the Google Drive folder name (e.g. `colab drive download`). Leave blank for auto-discovery.
enable_debug_logs = True #@param {type:"boolean"}

import time
import logger
logger.set_debug(enable_debug_logs)
import helper_worker

print("=" * 72)
print("🟢 Launching Autonomous GPU Worker Node...")
print(f"   Coordinator URL: {coordinator_url if coordinator_url else 'Auto-Discover (via Google Drive master.json)'}")
if drive_folder_name:
    print(f"   Drive Folder:    {drive_folder_name}")
print(f"   Debug Log Notifications: {'ENABLED (Verbose)' if enable_debug_logs else 'DISABLED (Quiet)'}")
print("   Keep this cell running to provide GPU compute.")
print("=" * 72 + "\\n")

try:
    helper_worker.run_helper_main(coordinator_url=coordinator_url, drive_folder_name=drive_folder_name, enable_debug_logs=enable_debug_logs)
except KeyboardInterrupt:
    print("\\n🛑 Worker stopped by user.")
""")

    nb = {
        "nbformat": 4,
        "nbformat_minor": 0,
        "metadata": {
            "colab": {
                "name": "drive_downloader_gpu_helper.ipynb",
                "provenance": [],
                "gpuType": "T4"
            },
            "kernelspec": {
                "name": "python3",
                "display_name": "Python 3"
            },
            "language_info": {
                "name": "python"
            },
            "accelerator": "GPU"
        },
        "cells": cells
    }

    out_path = os.path.join(os.path.dirname(__file__), "drive_downloader_gpu_helper.ipynb")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=2)

    print(f"[OK] Generated {out_path} successfully ({len(cells)} cells).")

if __name__ == "__main__":
    generate_helper_notebook()
