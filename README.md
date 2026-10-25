# 🎬 Colab Video Downloader & 480p Ultra-Compact MKV Batch Engine

An asynchronous batch video processing web application running natively in Google Colab with an interactive **Gradio** dashboard, hardware NVENC GPU encoding, and direct Google Drive preservation.

---

## 🌟 Key Architecture & Capabilities

### 1. Decluttered 3-Tab Interface (Zero Visual Clutter)
* **Tab 1: 🔎 Instant Search & Episode Discovery**:
  * Search any movie or TV series by name (e.g. `FBI`, `Inception`, `Breaking Bad`, `Stranger Things`, `Reacher`).
  * Live suggestions powered by IMDb's official suggestion engine (free, no API key required).
  * TV Series Season picker & full episode checklist with titles (e.g. `EP 01 - Pilot`, `EP 02 - Green Birds`).
  * **Multi-Tracker Torrent Swarms**: Real-time swarm search across The Pirate Bay, YTS, 1337x, and public trackers with quality tags (`[4K]`, `[1080p]`, `[720p]`, `[SD]`) and seed/peer health counts.
  * 1-Click batch season queueing with auto-naming (`FBI S01E03 - Prey_480p.mkv`).
* **Tab 2: 🧲 Torrent & Magnet Explorer (Single Unified Hub)**:
  * Eliminates duplicate input fields — this is the single, dedicated hub for all torrent operations.
  * Paste a magnet link or upload `.torrent` files.
  * Pure-Python Bencode inspector decodes metadata, calculates total size, and renders the complete folder/file tree.
  * Interactive file selector lets you choose specific episodes or whole folders.
  * Downloads via `aria2c` with selective file filtering (`--select-file=N`), zero seeding (`--seed-time=0`), and protocol encryption.
  * Preserves exact folder hierarchy in Google Drive (`/content/drive/MyDrive/vid/<TorrentName>/<Folder>/<File>_480p.mkv`).
  * Optional cloud debrid integration (TorBox / Real-Debrid) for 100% P2P-insulated direct downloads.
* **Tab 3: 🔗 Direct Links & Custom Streams**:
  * Direct MP4/MKV download links.
  * Videm embed links (`https://videm.xyz/embed/...` or `/player.php?id=...`).
  * VidSrc / vsembed stream links with automated TMDb-to-IMDb translation and scene release naming.
  * HLS M3U8 live playlists.
  * YouTube, Vimeo, Doodstream, Streamtape, and other web video platforms via `yt-dlp`.

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
