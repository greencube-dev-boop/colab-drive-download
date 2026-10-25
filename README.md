# 🎬 Colab Video Downloader & Universal Transcode Engine

An asynchronous batch video processing web application running natively in Google Colab with an interactive **Gradio** dashboard, hardware NVENC GPU encoding, and direct Google Drive preservation.

---

## 🌟 Key Architecture & Capabilities

### 1. ⚙️ Universal Settings Bar (Top Persistent Controls)
* **📥 Download Settings**:
  * **Download Connections / Chunks**: Configurable slider (1 to 16 concurrent streams for aria2c and HLS).
  * **Cloud Debrid Bridge**: Optional TorBox or Real-Debrid routing for 100% P2P-insulated direct downloads.
* **🎚️ Output Transcoding & Container Options**:
  * **Resolution Presets**: `480p (Default - Ultra Compact)`, `360p (Mobile Ultra-Low)`, `240p (Data Saver)`, `720p (HD)`, `1080p (Full HD)`, or `Original / Passthrough`.
  * **Video Codecs**: `H.265 / HEVC` (high compression) or `H.264 / AVC` (broad device compatibility) with GPU NVENC hardware acceleration and transparent CPU fallback.
  * **Container Formats**: `MKV` (multi-track resilient) or `MP4` (`+faststart` streaming).
  * **Audio Presets**: `Opus 64 kbps` (recommended zero-drift audio), `Opus 48 kbps`, `AAC 96 kbps`, `AAC 128 kbps`, or `Copy Original Audio`.
  * **Parallel Conversion Chunks**: Configurable worker threads (1 to 6 GPU/CPU slices).
  * **Quality CRF Slider**: Customizable visual quality (18 to 40, default: 32).

### 2. 2-Tab Layout with Instant Navigation
* **Main Tab 1: 📥 Fetch & Queue**:
  * **Sub-Tab 1: 🔗 Direct Download**: Paste direct MP4/MKV links, Videm/VidSrc embeds, and HLS M3U8 playlists.
  * **Sub-Tab 2: 🧲 Torrent & Magnet Download**: Unified hub for magnet links or `.torrent` file uploads with folder hierarchy tree, selective episode picking, and Drive folder preservation.
  * **Sub-Tab 3: 🔎 Title Search & Swarm Explorer**: Search any movie or TV series, browse official episodes, and query live multi-tracker torrent swarms across The Pirate Bay (`apibay.org`) and YTS.
* **Main Tab 2: 📊 Downloads & Processes**:
  * **Automated Tab Switching**: Adding downloads from any sub-tab instantly switches to this tab!
  * **Working 1-Click Clear & Cancel**: Immediate subprocess termination (`ffmpeg`, `aria2c`), workspace cleanup, and instant removal from the process cards.
  * Single **`🧹 Clear Completed`** button to clean all finished/errored tasks.

### 2. Reliable Stream & Embed Ingestion
* **Videm 5-Server Multi-Probing**:
  * Simultaneously tests all 5 upstream server endpoints (Server XPM1, Server VNH, Server AL, Server AK, Server PL) to bypass isolated 502/offline nodes.
  * Sliding-window HLS downloader with 8 concurrent workers, automatic token refresh (`fresh=1`) on CDN expiration, and 120-byte PNG camouflage unmasking on the fly.
* **VidSrc / vsembed Engine**:
  * Resolves stream sources from `data.vidsrc.sh` and maps TMDb identifiers to official IMDb series metadata.

### 3. Responsive Process Controls & Workspace Cleanup
* **Working 1-Click Cancel Button (`✕ Clear`)**:
  * Located directly on every process card header.
  * Interacts with a native FastAPI route (`POST /api/task/{id}/cancel`).
  * Immediately terminates active subprocesses (`aria2c`, `ffmpeg`, `yt-dlp`).
  * Cleans up all task temporary chunks, audio streams, and raw video files from disk.
  * Releases download/conversion semaphores immediately.
* **Clean Process Panel**:
  * Removed redundant top-menu clear buttons; now features a single, prominent `🧹 Clear Completed` button.
  * Real-time visual progress tiles (download %, speed, ETA, parallel GPU chunk bars).

### 4. GPU Acceleration & Zero-Desync Audio
* **Continuous Opus Audio**:
  * Audio is extracted and encoded once as a single continuous `audio.opus` file. Slicing video without slicing audio guarantees 100% frame-perfect sync with zero seam clicks or drift.
* **Hardware Accelerated Transcoding**:
  * Employs `-hwaccel cuda` and `hevc_nvenc` on Colab's Tesla T4 with `-cq:v 32 -b:v 0 -maxrate 550k` (Ultra-Compact Profile: ~180–230 MB for an 80–90 min video).
  * Transparent CPU fallback (`libx265`) if running in a CPU-only environment.

### 5. Persistent Cell Keep-Alive Heartbeat
* Includes an active **heartbeat keep-alive loop** running continuously in the foreground (`[*]`). It prints a live in-place status line every 5 seconds (uptime, active downloads, conversions, and completed count), ensuring Colab stays actively busy for up to the full 12-hour session limit without timing out.

---

## 📁 Modular Project Structure

The project has been refactored into a clean, maintainable modular structure:

```
colab-drive-download/
├── config.py                 # Paths, directories, dual semaphores, task state
├── torrent_parser.py         # Pure-Python bencode decoder, metadata & folder inspector
├── torrent_search.py         # en.yts.lu multi-tracker torrent search & quality classifier
├── media_search.py           # IMDb & TVmaze search, seasons & episode discovery
├── resolvers.py              # Videm 5-server probing, VidSrc, Real-Debrid, TorBox
├── downloader.py             # Sliding-window HLS, aria2c multi-connection, selective torrents
├── encoder.py                # GPU NVENC 480p encoding, continuous Opus, folder preservation
├── pipeline.py               # Asynchronous task scheduler, queue worker, task cancellation
├── ui.py                     # 3-tab Gradio UI, native FastAPI cancel route, CSS/JS
├── app.py                    # Server launcher CLI (--port, --share) & telemetry loop
├── build_gradio_notebook.py  # Builder script for video_to_480p_mkv_drive.ipynb
├── video_to_480p_mkv_drive.ipynb # Clean 2-cell Google Colab notebook
├── requirements.txt          # Minimal runtime dependencies
└── README.md                 # Project documentation & usage instructions
```

---

## 🚀 How to Run in Google Colab

### Method A: Use the 2-Cell Notebook Directly
1. Open [Google Colab](https://colab.research.google.com/).
2. Click **File > Upload notebook** and select [`video_to_480p_mkv_drive.ipynb`](video_to_480p_mkv_drive.ipynb).
3. Set your runtime to GPU: **Runtime > Change runtime type > T4 GPU**.
4. Run **Step 1**: Mounts Google Drive, installs dependencies, and syncs the project from GitHub (or extracts the embedded fallback bundle).
5. Run **Step 2**: Launches the Gradio web interface with a public link and starts the keep-alive telemetry loop.

---

## 🔄 How to Push & Sync with Your GitHub Repository

Because the code is now fully modularized and tracked with Git, you can push it to your personal or organization GitHub repository:

### 1. Repository
The project repository is hosted at:
**[https://github.com/greencube-dev-boop/colab-drive-download](https://github.com/greencube-dev-boop/colab-drive-download)**

### 2. Connect & Push Local Git to GitHub
In your terminal inside `d:\code\colab drive download`:
```bash
# Add GitHub remote URL
git remote add origin https://github.com/greencube-dev-boop/colab-drive-download.git

# Push code to GitHub
git push -u origin main
```

### 3. Update the Repository URL in the Notebook (Optional)
If you fork or host this on your own GitHub account, update `REPO_URL` in `build_gradio_notebook.py` (line 122):
```python
REPO_URL = "https://github.com/<your-username>/<your-repo-name>.git"
```
Then run:
```bash
python build_gradio_notebook.py
```
This regenerates `video_to_480p_mkv_drive.ipynb` so that whenever Step 1 is executed in Colab, it will automatically `git clone` or `git pull` the latest updates from your repository!
