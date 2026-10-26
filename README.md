# 🚀 Two-Stage Google Drive Downloader & GPU Refiner

An enterprise-grade, asynchronous batch media ingestion and transcoding system running natively in Google Colab with an interactive **Gradio** dashboard, persistent SQLite queue, Gemini AI media classification, hardware NVENC GPU encoding, and direct Google Drive preservation.

---

## 🌟 Key Architecture & Capabilities

### 1. ⚙️ Universal Settings Bar (Top Persistent Header)
* **📥 Ingestion & Download Rules**:
  * **Aria2c Connections / Stream Chunks**: Configurable slider (1 to 16 concurrent streams for aria2c and HLS).
  * **Max Concurrent Downloads**: Real-time semaphore controller (1 to 5 parallel downloads).
  * **Debrid Acceleration**: Support for Real-Debrid, AllDebrid, TorBox, and Debrid-Link.
* **🎚️ Output Transcoding & Container Options**:
  * **Resolution Presets**: `480p (Default - Ultra Compact)`, `360p (Mobile Ultra-Low)`, `240p (Data Saver)`, `720p (HD)`, `1080p (Full HD)`, or `Original / Passthrough`.
  * **Video Codecs**: `H.265 / HEVC` (high compression) or `H.264 / AVC` (broad device playback) with GPU NVENC hardware acceleration and transparent CPU fallback.
  * **Container Formats**: `MKV` (multi-track resilient) or `MP4` (`+faststart` streaming).
  * **Audio Presets**: `Opus 64 kbps` (zero-desync frame-perfect), `Opus 48 kbps`, `AAC 128 kbps`, `AAC 96 kbps`, or `Direct Copy (Passthrough)`.
  * **GPU VRAM Slicing Chunks**: Slicing slider (1 to 8 parallel GPU chunks).
  * **Quality CRF Slider**: Customizable visual quality factor (18 to 36, default: 32).
  * **Max Concurrent Refiners**: Worker thread limit (1 to 3 parallel transcode jobs).

---

### 2. Two-Stage Decoupled Workflow

```
[Fetch & Ingest] 
      │
      ▼
Stage 1: Ingestion
  1. Download video directly into local VM ephemeral disk (`/content/temp/raw_in/`)
  2. Classify via Gemini AI or scene heuristic regex:
     • Movie  -> "Movie Title (Year)" (e.g. "Avatar Fire And Ash (2025)")
     • TV     -> "Show Name S01E04" (e.g. "Mentalist S01E04")
     • Other  -> Clean title
  3. Move raw video to Google Drive: `drive/downloader/raw/<standardized_name>.<ext>`
  4. Record entry in `drive/downloader/data/metadata.json` (status: "raw")
  5. Enqueue into persistent SQLite database: `drive/downloader/data/queue.db` (`refine_queue`)
      │
      ▼
Stage 2: Persistent Refining (Background Daemon)
  1. Daemon continuously monitors `refine_queue` in `queue.db`
  2. Copies video from Drive `raw/` to Colab VM disk (`/content/temp/refining/`)
  3. Chunks video, loads slices into GPU VRAM, encodes with NVENC
  4. Stitches refined chunks with continuous Opus audio
  5. Moves refined file to Google Drive:
     • TV:    `drive/downloader/refined/tv_series/<Show Name>/Season <NN>/<Show SxxExx>.<ext>`
     • Movie: `drive/downloader/refined/movies/<Movie (Year)>.<ext>`
     • Other: `drive/downloader/refined/other/<Clean Title>.<ext>`
  6. Verifies destination file exists on Google Drive and is > 0 bytes
  7. **Drive Trash Bin Bypass**: Moves raw video from Drive to local VM disk, then unlinks locally
  8. Updates status to "refined" in `metadata.json` and marks job "completed" in `queue.db`
```

---

### 3. Responsive 3-Tab Interface with Auto-Switch

* **Tab 1: 📥 Fetch & Ingest**:
  * **Sub-Tab 1: 🔗 Direct & Streaming Download**: Direct MP4/MKV links, Videm/VidSrc embeds, and HLS M3U8 playlists.
  * **Sub-Tab 2: 🧲 Torrent & Magnet Download**: Unified hub for magnet links or `.torrent` file uploads with interactive folder hierarchy tree and selective file picking.
  * **Sub-Tab 3: 🔎 Title Search & Swarm Explorer**: Search any movie or TV series, browse official episodes, and query live multi-tracker torrent swarms across The Pirate Bay (`apibay.org`) and YTS (`en.yts.lu`).
  * *Adding any download automatically switches to Tab 2.*

* **Tab 2: 📋 Queue & Process Manager**:
  * **Sub-Tab 1: 📥 Download Queue**: Real-time download speeds, progress %, and upcoming queued downloads with **⬆️ Up** and **⬇️ Down** reordering buttons + single `🧹 Clear Completed`.
  * **Sub-Tab 2: ⚙️ Refining Queue (Persistent queue.db)**: Live GPU transcode progress, parallel VRAM chunk mini-bars, and upcoming jobs with **⬆️ Up** and **⬇️ Down** reorder buttons, pause/resume toggle, and single `🧹 Clear Completed`.
  * *If a Colab session ends or disconnects, pending refining jobs automatically resume in the next session without re-downloading!*

* **Tab 3: 📚 Drive Media Catalog**:
  * Live interactive browser reading `drive/downloader/data/metadata.json` showing:
    * 🎬 Movies with `[RAW]` vs `[REFINED]` status badges.
    * 📺 TV Series organized by show name $\rightarrow$ seasons $\rightarrow$ episodes.
    * 📁 Other media.

---

## 📁 Modular Project Structure

```
colab-drive-download/
├── config.py                        # Path layouts, semaphores, trash bypass logic
├── database.py                      # SQLite queue manager (queue.db) with reordering
├── metadata_manager.py              # Master metadata.json catalog manager
├── classifier.py                    # Gemini AI & regex title standardizer
├── torrent_parser.py                # Pure-Python bencode decoder & folder tree parser
├── torrent_search.py                # Multi-tracker swarm search (APIBay & YTS.lu)
├── media_search.py                  # IMDb & TVmaze search, seasons & episode discovery
├── resolvers.py                     # Videm 5-server multi-probing, VidSrc, Debrid
├── downloader.py                    # Sliding-window HLS & aria2c multi-connection
├── encoder.py                       # GPU NVENC transcode, chunked VRAM, continuous audio
├── pipeline.py                      # Two-stage scheduler & persistent refining daemon
├── ui.py                            # 3-tab Gradio UI with universal header & live updates
├── app.py                           # Application entry point with keep-alive loop
├── build_gradio_notebook.py         # Notebook compiler script
├── drive_downloader_gpu_refiner.ipynb # The clean 2-cell Google Colab notebook
├── test_suite.py                    # Comprehensive unit tests
├── requirements.txt                 # Runtime dependencies
└── README.md                        # Documentation
```

---

## 🚀 How to Run in Google Colab

1. Open [Google Colab](https://colab.research.google.com/).
2. Click **File > Upload notebook** and select [`drive_downloader_gpu_refiner.ipynb`](drive_downloader_gpu_refiner.ipynb).
3. Set your runtime to GPU: **Runtime > Change runtime type > T4 GPU**.
4. Run **Step 1**: Mounts Google Drive, installs system packages (`aria2`, `ffmpeg` with NVENC), and syncs the project from GitHub.
5. Run **Step 2**: Launches the Gradio web interface with a public link and starts the keep-alive telemetry loop.
