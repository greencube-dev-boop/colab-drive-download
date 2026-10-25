import os
import re
import html
import time
import urllib.parse
from typing import Dict, Any, List, Tuple
import gradio as gr
from config import tasks, tasks_lock, sanitize_filename
import config
from torrent_parser import format_torrent_size, fetch_torrent_metadata, extract_file_path
from torrent_search import search_live_torrents
from media_search import search_media_imdb, get_tv_seasons_data
from pipeline import cancel_task_by_id, clear_done_tasks, queue_direct_items, run_task_pipeline
import threading

def render_dashboard_html() -> str:
    """Renders real-time process cards with modern dark-mode styling and functioning 1-click cancel buttons."""
    with tasks_lock:
        current_tasks = list(config.tasks)

    if not current_tasks:
        return """
        <div style="text-align: center; padding: 48px 20px; background: #16161e; border: 1px dashed #313244; border-radius: 12px; color: #7f849c;">
            <div style="font-size: 36px; margin-bottom: 8px;">🎬</div>
            <div style="font-size: 16px; font-weight: 600; color: #a6adc8;">No active or queued video processes</div>
            <div style="font-size: 13px; color: #6c7086; margin-top: 4px;">Use the <b>📥 Fetch & Queue</b> tab to search titles, explore torrents, or paste direct links.</div>
        </div>
        """

    status_colors = {
        "Queued for Download": {"bg": "#313244", "fg": "#bac2de", "border": "#45475a"},
        "Downloading": {"bg": "#1e3a5f", "fg": "#89b4fa", "border": "#89b4fa"},
        "Queued for Conversion": {"bg": "#3e3b24", "fg": "#f9e2af", "border": "#f9e2af"},
        "Converting": {"bg": "#38234a", "fg": "#cba6f7", "border": "#cba6f7"},
        "Moving to Drive": {"bg": "#3d2b1f", "fg": "#fab387", "border": "#fab387"},
        "Done": {"bg": "#1e382b", "fg": "#a6e3a1", "border": "#a6e3a1"},
        "Error": {"bg": "#3b1e28", "fg": "#f38ba8", "border": "#f38ba8"},
        "Cancelled": {"bg": "#2e2e38", "fg": "#9399b2", "border": "#585b70"}
    }

    cards_html = ['<div style="display: flex; flex-direction: column; gap: 14px; font-family: system-ui, -apple-system, sans-serif;">']

    for task in reversed(current_tasks):
        st = task.get("status", "Queued for Download")
        sc = status_colors.get(st, status_colors["Queued for Download"])
        title = html.escape(task.get("title", task.get("url", "Video")))
        task_id = task.get("id", 0)

        folder_badge = ""
        if task.get("rel_folder") and task["rel_folder"] != "Root":
            folder_badge = f'<span style="background: #313244; color: #a6adc8; border: 1px solid #45475a; font-size: 11px; padding: 2px 8px; border-radius: 6px; margin-left: 6px;">📁 {html.escape(task["rel_folder"])}</span>'

        res_raw = str(task.get("res_preset", "480p"))
        res_tag = res_raw.split()[0] if res_raw else "480p"
        codec_tag = "H.265" if "265" in str(task.get("video_codec", "H.265")) else "H.264"
        cont_tag = str(task.get("container_format", "mkv")).upper()
        fmt_badge = f'<span style="background: rgba(203, 166, 247, 0.12); color: #cba6f7; border: 1px solid rgba(203, 166, 247, 0.3); font-size: 11px; padding: 2px 8px; border-radius: 6px; margin-left: 6px;">⚙️ {res_tag} {cont_tag} • {codec_tag}</span>'

        btn_label = "✕ Clear" if st in ["Done", "Error", "Cancelled"] else "✕ Cancel"

        card = [f"""
        <div id="task-card-{task_id}" style="background: #1e1e2e; border: 1px solid {sc['border']}; border-radius: 12px; padding: 16px 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.25); transition: opacity 0.3s ease;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                <div style="font-weight: 600; font-size: 15px; color: #cdd6f4; max-width: 68%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                    🎬 #{task_id}: {title}{folder_badge}{fmt_badge}
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="background: {sc['bg']}; color: {sc['fg']}; border: 1px solid {sc['border']}; font-weight: 600; font-size: 12px; padding: 4px 12px; border-radius: 20px;">
                        {st}
                    </span>
                    <button id="cancel-btn-{task_id}"
                            onclick="window.cancelTask({task_id})"
                            title="Cancel and remove process #{task_id}"
                            style="background: rgba(243, 139, 168, 0.15); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.4); font-weight: 600; font-size: 11px; padding: 4px 10px; border-radius: 6px; cursor: pointer; transition: all 0.2s ease;">
                        {btn_label}
                    </button>
                </div>
            </div>
        """]

        if st == "Downloading":
            pct = task.get("download_pct", 0.0)
            spd = task.get("download_speed", "--")
            dl_str = task.get("downloaded_str", "0 MB")
            tot_str = task.get("total_str", "Unknown")
            card.append(f"""
            <div style="background: #313244; border-radius: 6px; height: 12px; width: 100%; overflow: hidden; margin: 8px 0;">
                <div style="background: linear-gradient(90deg, #89b4fa, #b4befe); height: 100%; width: {pct:.1f}%; transition: width 0.3s ease;"></div>
            </div>
            <div style="display: flex; justify-content: space-between; font-size: 13px; color: #a6adc8;">
                <span>Downloaded: <b>{pct:.1f}%</b> ({dl_str} / {tot_str})</span>
                <span>⚡ Speed: <b>{spd}</b></span>
            </div>
            """)

        elif st == "Converting":
            chunks = task.get("chunk_status", {})
            total_curr = sum(c.get("curr_sec", 0) for c in chunks.values()) if chunks else 0
            total_dur = sum(c.get("dur_sec", 1) for c in chunks.values()) if chunks else 1
            overall_pct = (total_curr / total_dur) * 100 if total_dur > 0 else 0

            dur_str = f"{int(total_dur // 60)}m {int(total_dur % 60)}s" if total_dur > 60 else f"{int(total_dur)}s"
            curr_str = f"{int(total_curr // 60)}m {int(total_curr % 60)}s" if total_curr > 60 else f"{int(total_curr)}s"

            speeds = [c.get("speed") for c in chunks.values() if c.get("speed") and c.get("speed") != "--"]
            disp_speed = speeds[-1] if speeds else "--"

            card.append(f"""
            <div style="background: #313244; border-radius: 6px; height: 12px; width: 100%; overflow: hidden; margin: 8px 0;">
                <div style="background: linear-gradient(90deg, #cba6f7, #f5c2e7); height: 100%; width: {overall_pct:.1f}%; transition: width 0.3s ease;"></div>
            </div>
            <div style="display: flex; justify-content: space-between; font-size: 13px; color: #a6adc8; margin-bottom: 8px;">
                <span>Transcoding to {res_tag} {cont_tag}: <b>{overall_pct:.1f}%</b> ({curr_str} / {dur_str})</span>
                <span>⚡ Rate: <b>{disp_speed}</b></span>
            </div>
            """)

            if chunks and len(chunks) > 1:
                card.append("""
                <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 8px 12px; margin-top: 6px;">
                    <div style="font-size: 11px; font-weight: 600; color: #a6adc8; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px;">Parallel Transcoding Chunks:</div>
                    <div style="display: flex; flex-direction: column; gap: 6px;">
                """)
                for c_idx, c_info in sorted(chunks.items()):
                    c_pct = c_info.get("pct", 0.0)
                    c_spd = c_info.get("speed", "--")
                    c_st = c_info.get("status", "Queued")
                    card.append(f"""
                        <div style="display: flex; align-items: center; gap: 8px; font-size: 12px;">
                            <span style="width: 65px; color: #bac2de; font-weight: 500;">Chunk {c_idx + 1}:</span>
                            <div style="flex-grow: 1; background: #313244; height: 6px; border-radius: 3px; overflow: hidden;">
                                <div style="background: #cba6f7; width: {c_pct:.1f}%; height: 100%;"></div>
                            </div>
                            <span style="width: 45px; text-align: right; color: #cdd6f4; font-size: 11px;">{c_pct:.0f}%</span>
                            <span style="width: 60px; text-align: right; color: #a6adc8; font-size: 11px;">{c_spd}</span>
                            <span style="font-size: 10px; color: #6c7086; width: 50px; text-align: right;">{c_st}</span>
                        </div>
                    """)
                card.append("</div></div>")

        elif st == "Moving to Drive":
            card.append("""
            <div style="background: #313244; border-radius: 6px; height: 8px; width: 100%; overflow: hidden; margin: 8px 0;">
                <div style="background: linear-gradient(90deg, #fab387, #f9e2af); height: 100%; width: 100%; animation: pulse 1.5s infinite;"></div>
            </div>
            <div style="font-size: 13px; color: #fab387;">🚀 Verifying checksums & saving to Google Drive...</div>
            """)

        elif st == "Done":
            raw_mb = task.get("raw_size_mb", 0.0)
            final_mb = task.get("final_size_mb", 0.0)
            saved_pct = ((raw_mb - final_mb) / raw_mb * 100) if raw_mb > 0 and raw_mb > final_mb else 0.0
            drive_p = html.escape(task.get("drive_path", "Google Drive: /content/drive/MyDrive/vid/"))
            card.append(f"""
            <div style="font-size: 13px; color: #a6e3a1; margin-top: 4px;">
                💾 <b>Saved to Google Drive:</b> <code style="background: #11111b; padding: 2px 6px; border-radius: 4px; color: #89dceb; word-break: break-all;">{drive_p}</code>
            </div>
            <div style="display: flex; gap: 15px; font-size: 12px; color: #bac2de; margin-top: 6px;">
                <span>Original: <b>{raw_mb:.1f} MB</b></span>
                <span>Output: <b>{final_mb:.1f} MB</b></span>
                <span style="color: #a6e3a1; font-weight: 600;">⚡ Saved: <b>{saved_pct:.1f}% space saved</b></span>
            </div>
            """)

        elif st == "Error":
            err_msg = html.escape(task.get("error_msg", "Unknown error during processing"))
            card.append(f"""
            <div style="background: #28141d; border-left: 3px solid #f38ba8; padding: 8px 12px; border-radius: 4px; margin-top: 8px; font-size: 12px; color: #f38ba8; word-break: break-word;">
                ❌ <b>Error:</b> {err_msg}
            </div>
            """)

        elif st == "Cancelled":
            card.append("""
            <div style="font-size: 12px; color: #9399b2; margin-top: 4px;">
                🛑 Process cancelled by user. Workspace raw & temp files cleaned up.
            </div>
            """)

        card.append("</div>")
        cards_html.append("\n".join(card))

    cards_html.append("</div>")
    return "\n".join(cards_html)

HEAD_JS = """
<script>
window.cancelTask = function(taskId) {
    var card = document.getElementById('task-card-' + taskId);
    if (card) {
        card.style.opacity = '0.3';
        card.style.pointerEvents = 'none';
        var btn = document.getElementById('cancel-btn-' + taskId);
        if (btn) {
            btn.innerText = 'Removing...';
            btn.disabled = true;
        }
    }
    // 1. Call native FastAPI cancel route
    fetch('/api/task/' + taskId + '/cancel', { method: 'POST' })
        .then(function(res) { return res.json(); })
        .then(function(data) {
            if (card) card.remove();
        })
        .catch(function(err) {
            console.error('Cancel API error:', err);
        });

    // 2. Redundant dispatch to hidden Gradio component
    try {
        var inp = document.querySelector('#hidden_cancel_input textarea, #hidden_cancel_input input');
        var btnAction = document.querySelector('#hidden_cancel_btn');
        if (inp && btnAction) {
            inp.value = taskId;
            inp.dispatchEvent(new Event('input', { bubbles: true }));
            btnAction.click();
        }
    } catch(e) {}
};
</script>
<style>
/* Modern Sleek Dark Mode Overrides */
.tabs { border-bottom: 2px solid #313244 !important; }
.tab-nav button.selected { border-bottom: 2px solid #cba6f7 !important; color: #cba6f7 !important; font-weight: 700 !important; }
</style>
"""

def create_ui() -> gr.Blocks:
    """Constructs the unified 2-tab Gradio UI with top Universal Settings bar and automated downloads view."""

    with gr.Blocks(title="🎬 Colab Video Downloader & Universal Video Engine", head=HEAD_JS, theme=gr.themes.Soft(primary_hue="purple", neutral_hue="slate")) as demo:

        gr.Markdown("""
        # 🎬 Colab Video Downloader & Adaptive Transcode Engine
        **High-speed parallel downloads with Tesla T4 NVENC GPU acceleration and direct Google Drive preservation.**
        """)

        # -------------------------------------------------------------
        # ⚙️ UNIVERSAL SETTINGS BAR (Persistent Across All Tabs)
        # -------------------------------------------------------------
        with gr.Accordion("⚙️ Universal Download & Transcoding Settings (Applied to all queued items)", open=True):
            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("##### 📥 Download Engine Settings")
                    download_conn_slider = gr.Slider(minimum=1, maximum=16, value=8, step=1, label="⚡ Download Connections / Chunks (aria2c / HLS)")
                    debrid_provider = gr.Dropdown(
                        choices=["Local aria2c (Direct / Encrypted)", "TorBox (Cloud Seedbox)", "Real-Debrid"],
                        value="Local aria2c (Direct / Encrypted)",
                        label="🛡️ Torrent Cloud Debrid Provider"
                    )
                    debrid_api_key = gr.Textbox(placeholder="Optional API key for TorBox / Real-Debrid", type="password", label="🔑 Debrid API Key")

                with gr.Column(scale=2):
                    gr.Markdown("##### 🎚️ Output Transcode & Format Settings")
                    with gr.Row():
                        res_preset = gr.Dropdown(
                            choices=[
                                "480p (Default - Ultra Compact)",
                                "360p (Mobile Ultra-Low)",
                                "240p (Data Saver)",
                                "720p (HD)",
                                "1080p (Full HD)",
                                "Original / Passthrough"
                            ],
                            value="480p (Default - Ultra Compact)",
                            label="📐 Target Resolution"
                        )
                        v_codec = gr.Dropdown(
                            choices=[
                                "H.265 / HEVC (Recommended - High Compression)",
                                "H.264 / AVC (Universal Device Compatibility)"
                            ],
                            value="H.265 / HEVC (Recommended - High Compression)",
                            label="🎞️ Video Codec"
                        )
                        container_format = gr.Dropdown(
                            choices=[
                                "MKV (Default - Multi-Track & Resilient)",
                                "MP4 (Universal Streaming)"
                            ],
                            value="MKV (Default - Multi-Track & Resilient)",
                            label="📦 Container Format"
                        )
                    with gr.Row():
                        audio_preset = gr.Dropdown(
                            choices=[
                                "Opus 64 kbps (Recommended - Zero Drift)",
                                "Opus 48 kbps (Ultra-Compact Voice/Audio)",
                                "AAC 96 kbps (Universal)",
                                "AAC 128 kbps (High Fidelity)",
                                "Copy Original Audio (Passthrough)"
                            ],
                            value="Opus 64 kbps (Recommended - Zero Drift)",
                            label="🎵 Audio Track & Bitrate"
                        )
                        workers_slider = gr.Slider(minimum=1, maximum=6, value=3, step=1, label="⚡ Parallel Transcode Chunks (GPU/CPU)")
                        crf_slider = gr.Slider(minimum=18, maximum=40, value=32, step=1, label="🎚️ Compression CRF (Lower = Better Quality, Higher = Smaller File)")

        # -------------------------------------------------------------
        # MAIN 2 TABS: 📥 Fetch & Queue  vs  📊 Downloads & Processes
        # -------------------------------------------------------------
        with gr.Tabs(selected="tab_fetch") as main_tabs:

            # =========================================================
            # MAIN TAB 1: 📥 Fetch & Queue (3 Sub-Tabs)
            # =========================================================
            with gr.Tab("📥 Fetch & Queue", id="tab_fetch"):
                with gr.Tabs(selected="subtab_direct") as fetch_subtabs:

                    # -------------------------------------------------
                    # Sub-Tab 1: 🔗 Direct Download
                    # -------------------------------------------------
                    with gr.Tab("🔗 Direct Download", id="subtab_direct"):
                        with gr.Row():
                            with gr.Column(scale=4):
                                direct_input_box = gr.Textbox(
                                    label="🔗 Video / Embed Links (Comma or newline separated)",
                                    placeholder="https://example.com/video.mp4\nhttps://videm.xyz/embed/movie/tt1375666\nhttps://vidsrc.xyz/embed/tv?imdb=tt7491982&season=1&episode=1",
                                    lines=4
                                )
                                direct_name_box = gr.Textbox(
                                    label="🏷️ Custom File Name(s) (Optional)",
                                    placeholder="e.g. Inception (2010) or Part1, Part2 (leave empty to auto-detect original title)",
                                    lines=1
                                )
                            with gr.Column(scale=1):
                                btn_add_direct = gr.Button("➕ Add to Queue", variant="primary", size="lg")
                                direct_queue_status = gr.Markdown("", visible=False)

                    # -------------------------------------------------
                    # Sub-Tab 2: 🧲 Torrent & Magnet Download
                    # -------------------------------------------------
                    with gr.Tab("🧲 Torrent & Magnet Download", id="subtab_torrent"):
                        with gr.Row():
                            with gr.Column(scale=3):
                                torrent_magnet_input = gr.Textbox(
                                    label="🧲 Magnet Link",
                                    placeholder="magnet:?xt=urn:btih:... or click 'Send to Torrent Explorer' from Search tab",
                                    lines=2
                                )
                            with gr.Column(scale=2):
                                torrent_file_input = gr.File(
                                    label="📁 Upload .torrent File",
                                    file_types=[".torrent"],
                                    type="filepath"
                                )

                        btn_inspect_torrent = gr.Button("🔍 Inspect Torrent Files & Folders", variant="secondary")

                        torrent_meta_state = gr.State({})
                        torrent_summary_html = gr.HTML("", visible=False)

                        with gr.Group(visible=False) as torrent_tree_group:
                            with gr.Row():
                                torrent_folder_filter = gr.Dropdown(
                                    label="📁 Filter by Folder",
                                    choices=["All Folders"],
                                    value="All Folders",
                                    scale=3
                                )
                                btn_select_entire_folder = gr.Button("✅ Select All in This Folder", scale=2)
                                btn_select_all_torrent = gr.Button("✅ Select All Videos", scale=2)
                                btn_deselect_all_torrent = gr.Button("◻️ Deselect All", scale=1)

                            torrent_videos_checkbox = gr.CheckboxGroup(
                                label="🎬 Videos in Selected Folder (Check individual videos to download)",
                                choices=[],
                                value=[]
                            )

                            with gr.Row():
                                btn_queue_torrent_videos = gr.Button("📥 Add Selected Video(s) to Queue", variant="primary", size="lg", scale=3)

                        torrent_action_msg = gr.Markdown("", visible=False)

                    # -------------------------------------------------
                    # Sub-Tab 3: 🔎 Search & Discovery
                    # -------------------------------------------------
                    with gr.Tab("🔎 Title Search & Swarm Explorer", id="subtab_search"):
                        with gr.Row():
                            search_input = gr.Textbox(
                                label="Movie or TV Show Name",
                                placeholder="e.g. Inception, FBI, Breaking Bad, Stranger Things, Reacher...",
                                scale=4
                            )
                            media_type_filter = gr.Radio(
                                choices=["All", "🎬 Movies", "📺 TV Series"],
                                value="All",
                                label="Category",
                                scale=2
                            )
                            btn_search = gr.Button("🔍 Search Title", variant="primary", scale=1)

                        search_status = gr.Markdown("Type a title above and press Search to discover movies, episodes, and torrent swarms.", visible=True)

                        media_select_dropdown = gr.Dropdown(
                            label="🎯 Search Results (Select a Movie or Series to load episodes/swarms)",
                            choices=[],
                            value=None,
                            interactive=True,
                            visible=False
                        )

                        search_cache = gr.State({})
                        selected_media_cache = gr.State({})
                        tv_seasons_cache = gr.State({})
                        live_torrents_cache = gr.State({})

                        with gr.Group(visible=False) as movie_panel:
                            with gr.Row():
                                movie_poster = gr.Image(label="Poster", width=140, height=200, show_label=False)
                                with gr.Column():
                                    movie_details = gr.Markdown("")
                                    with gr.Row():
                                        btn_queue_movie = gr.Button("🎬 Add Movie to Download Queue", variant="primary", scale=2)
                                        btn_insert_movie_direct = gr.Button("🔗 Copy to Direct Links", scale=1)

                        with gr.Group(visible=False) as tv_panel:
                            with gr.Row():
                                tv_poster = gr.Image(label="Poster", width=140, height=200, show_label=False)
                                with gr.Column():
                                    tv_details = gr.Markdown("")
                                    tv_season_select = gr.Dropdown(label="Season", choices=[], interactive=True)

                            with gr.Row():
                                btn_select_all_eps = gr.Button("✅ Select All Episodes in Season", scale=2)
                                btn_deselect_all_eps = gr.Button("◻️ Deselect All", scale=1)

                            tv_episodes_checkbox = gr.CheckboxGroup(label="Available Episodes (Check episodes to download)", choices=[], value=[])

                            with gr.Row():
                                btn_queue_selected_eps = gr.Button("📥 Queue Selected Episodes for Download", variant="primary", scale=2)
                                btn_insert_eps_direct = gr.Button("🔗 Copy Selected Episodes to Direct Links", scale=1)

                        # Swarm Indexer Box
                        with gr.Group(visible=False) as swarm_panel:
                            gr.Markdown("#### 🔥 Live Multi-Tracker Torrent Swarms (The Pirate Bay & YTS)")
                            with gr.Row():
                                live_torrents_dropdown = gr.Dropdown(
                                    label="Select Torrent Swarm",
                                    choices=[],
                                    interactive=True,
                                    scale=4
                                )
                                btn_refresh_torrents = gr.Button("🔄 Refresh Swarms", scale=1)
                            with gr.Row():
                                btn_quick_queue_swarm = gr.Button("⚡ 1-Click Queue This Swarm", variant="primary", scale=2)
                                btn_send_to_explorer = gr.Button("📂 Send to Torrent Explorer (Cherry-Pick Files)", variant="secondary", scale=2)

                        search_action_msg = gr.Markdown("", visible=False)

            # =========================================================
            # MAIN TAB 2: 📊 Downloads & Processes
            # =========================================================
            with gr.Tab("📊 Downloads & Processes", id="tab_downloads"):
                with gr.Row():
                    btn_clear_completed = gr.Button("🧹 Clear Completed", variant="secondary", scale=1)

                dashboard_component = gr.HTML(
                    value=render_dashboard_html(),
                    elem_id="dashboard_container"
                )

        # Periodic Dashboard Refresh (every 1 second)
        timer = gr.Timer(1.0)
        timer.tick(fn=render_dashboard_html, outputs=[dashboard_component])

        # Redundant Hidden Event Trigger for Web Browser / Iframe cancellation
        with gr.Row(visible=False):
            hidden_cancel_input = gr.Textbox(elem_id="hidden_cancel_input", visible=False)
            hidden_cancel_btn = gr.Button(elem_id="hidden_cancel_btn", visible=False)

        def handle_hidden_cancel(tid_val):
            if tid_val:
                try:
                    cancel_task_by_id(int(tid_val))
                except Exception:
                    pass
            return render_dashboard_html()

        hidden_cancel_btn.click(
            fn=handle_hidden_cancel,
            inputs=[hidden_cancel_input],
            outputs=[dashboard_component]
        )

        # -------------------------------------------------------------
        # EVENT HANDLERS
        # -------------------------------------------------------------

        # Direct Add to Queue Handler
        def handle_add_direct(urls_text, custom_names_text, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            raw_lines = [l.strip() for l in re.split(r'[\r\n,]+', urls_text or "") if l.strip()]
            valid_urls = [u for u in raw_lines if urllib.parse.urlparse(u).scheme in ['http', 'https', 'magnet']]

            if not valid_urls:
                return (
                    gr.update(), gr.update(),
                    render_dashboard_html(),
                    gr.update(visible=True, value="⚠️ No valid HTTP/HTTPS or Magnet links detected."),
                    gr.update()
                )

            name_lines = [n.strip() for n in re.split(r'[\r\n,]+', custom_names_text or "") if n.strip()]

            items = []
            if len(valid_urls) == 1 and len(name_lines) == 1:
                items.append((valid_urls[0], name_lines[0]))
            elif len(name_lines) == len(valid_urls):
                for u, n in zip(valid_urls, name_lines):
                    items.append((u, n))
            elif len(name_lines) == 1 and len(valid_urls) > 1:
                base = name_lines[0]
                for idx, u in enumerate(valid_urls, 1):
                    items.append((u, f"{base}_{idx:02d}"))
            else:
                for u in valid_urls:
                    items.append((u, ""))

            count = queue_direct_items(
                items,
                workers_val=int(workers),
                debrid_prov=str(debrid_p),
                debrid_k=str(debrid_k),
                download_conn=int(d_conn),
                res_preset=str(res_p),
                v_codec=str(v_cod),
                container=str(cont),
                audio_preset=str(aud_p),
                crf_val=int(crf_v)
            )

            # Auto-switch to Downloads tab!
            return (
                "", "",
                render_dashboard_html(),
                gr.update(visible=True, value=f"✅ Successfully queued {count} video(s)!"),
                gr.Tabs(selected="tab_downloads")
            )

        btn_add_direct.click(
            fn=handle_add_direct,
            inputs=[
                direct_input_box, direct_name_box,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                workers_slider, crf_slider
            ],
            outputs=[direct_input_box, direct_name_box, dashboard_component, direct_queue_status, main_tabs]
        )
        direct_input_box.submit(
            fn=handle_add_direct,
            inputs=[
                direct_input_box, direct_name_box,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                workers_slider, crf_slider
            ],
            outputs=[direct_input_box, direct_name_box, dashboard_component, direct_queue_status, main_tabs]
        )

        # Inspect Torrent Handler
        def handle_inspect_torrent(magnet_input, uploaded_file):
            source = None
            if uploaded_file:
                fp = extract_file_path(uploaded_file)
                if fp and os.path.exists(fp):
                    source = fp
            if not source and magnet_input and magnet_input.strip():
                source = magnet_input.strip()

            if not source:
                return (
                    gr.update(visible=False), "",
                    gr.update(choices=["All Folders"], value="All Folders"),
                    gr.update(choices=[], value=[]),
                    gr.update(visible=True, value="⚠️ Please paste a magnet link or upload a .torrent file to inspect."),
                    {}
                )
            try:
                meta = fetch_torrent_metadata(source)
                torrent_name = meta["name"]
                total_sz = meta["total_size_str"]
                video_count = meta["video_count"]
                folders = meta["folders"]
                files = meta["files"]

                if video_count == 0:
                    return (
                        gr.update(visible=False), "",
                        gr.update(choices=["All Folders"], value="All Folders"),
                        gr.update(choices=[], value=[]),
                        gr.update(visible=True, value=f"⚠️ No video files found in '{torrent_name}' ({len(files)} total files)."),
                        {}
                    )

                header_html = f"""
                <div style="background: rgba(147, 51, 234, 0.15); border-left: 4px solid #cba6f7; padding: 12px 16px; border-radius: 8px; margin-bottom: 12px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px;">
                        <span style="font-size: 16px; font-weight: 700; color: #cdd6f4;">🧲 {html.escape(torrent_name)}</span>
                        <div style="display: flex; gap: 10px; font-size: 13px;">
                            <span style="background: #313244; color: #89b4fa; padding: 3px 10px; border-radius: 6px; font-weight: 600;">🎬 {video_count} Videos</span>
                            <span style="background: #313244; color: #a6e3a1; padding: 3px 10px; border-radius: 6px; font-weight: 600;">💾 {total_sz}</span>
                            <span style="background: #313244; color: #bac2de; padding: 3px 10px; border-radius: 6px; font-weight: 600;">📁 {len(folders)} Folders</span>
                        </div>
                    </div>
                </div>
                """

                folder_choices = ["All Folders"] + folders
                video_choices = [f["label"] for f in files if f["is_video"]]

                return (
                    gr.update(visible=True),
                    header_html,
                    gr.update(choices=folder_choices, value="All Folders"),
                    gr.update(choices=video_choices, value=video_choices),
                    gr.update(visible=True, value=f"✅ Extracted metadata for **{torrent_name}**! Choose folders or select custom videos below to queue:"),
                    meta
                )
            except Exception as e:
                return (
                    gr.update(visible=False), "",
                    gr.update(choices=["All Folders"], value="All Folders"),
                    gr.update(choices=[], value=[]),
                    gr.update(visible=True, value=f"❌ Error fetching torrent metadata: {e}"),
                    {}
                )

        btn_inspect_torrent.click(
            fn=handle_inspect_torrent,
            inputs=[torrent_magnet_input, torrent_file_input],
            outputs=[torrent_tree_group, torrent_summary_html, torrent_folder_filter, torrent_videos_checkbox, torrent_action_msg, torrent_meta_state]
        )

        def handle_torrent_folder_change(selected_folder, meta, current_selected):
            if not meta or 'files' not in meta:
                return gr.update(choices=[], value=[])
            files = meta['files']
            if not selected_folder or selected_folder == "All Folders":
                choices = [f['label'] for f in files if f['is_video']]
            else:
                choices = [f['label'] for f in files if f['is_video'] and f['folder'] == selected_folder]
            val = [c for c in (current_selected or []) if c in choices]
            if not val:
                val = choices
            return gr.update(choices=choices, value=val)

        torrent_folder_filter.change(
            fn=handle_torrent_folder_change,
            inputs=[torrent_folder_filter, torrent_meta_state, torrent_videos_checkbox],
            outputs=[torrent_videos_checkbox]
        )

        def handle_select_entire_folder(selected_folder, meta, current_selected):
            if not meta or 'files' not in meta:
                return gr.update(value=[])
            files = meta['files']
            if not selected_folder or selected_folder == "All Folders":
                folder_labels = [f['label'] for f in files if f['is_video']]
            else:
                folder_labels = [f['label'] for f in files if f['is_video'] and f['folder'] == selected_folder]
            new_val = list(dict.fromkeys((current_selected or []) + folder_labels))
            return gr.update(value=new_val)

        btn_select_entire_folder.click(
            fn=handle_select_entire_folder,
            inputs=[torrent_folder_filter, torrent_meta_state, torrent_videos_checkbox],
            outputs=[torrent_videos_checkbox]
        )
        btn_select_all_torrent.click(
            fn=lambda meta: gr.update(value=[f['label'] for f in meta['files'] if f['is_video']]) if meta and 'files' in meta else gr.update(value=[]),
            inputs=[torrent_meta_state],
            outputs=[torrent_videos_checkbox]
        )
        btn_deselect_all_torrent.click(
            fn=lambda: gr.update(value=[]),
            outputs=[torrent_videos_checkbox]
        )

        # Queue Selected Torrent Videos
        def handle_queue_torrent_videos(selected_labels, meta, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not selected_labels or not meta or 'files' not in meta:
                return gr.update(visible=True, value="⚠️ Please select at least one video file from the list to download."), render_dashboard_html(), gr.update()

            files = meta['files']
            label_to_file = {f['label']: f for f in files if f['is_video']}
            torrent_name = meta['name']
            cached_meta_file = meta.get('cached_file_path')
            source_url = meta.get('source_url', '')

            items_to_queue = [label_to_file[l] for l in selected_labels if l in label_to_file]
            if not items_to_queue:
                return gr.update(visible=True, value="⚠️ No valid video files selected."), render_dashboard_html(), gr.update()

            queued_count = 0
            with tasks_lock:
                for f in items_to_queue:
                    config.task_counter += 1
                    queued_count += 1
                    file_title = f"{sanitize_filename(f['filename_clean'])}"
                    task_url = cached_meta_file if (cached_meta_file and os.path.isfile(cached_meta_file)) else source_url
                    new_task = {
                        "id": config.task_counter,
                        "url": task_url,
                        "title": file_title,
                        "has_custom_title": True,
                        "is_torrent_file": bool(cached_meta_file and os.path.isfile(cached_meta_file)),
                        "torrent_file_index": f["index"],
                        "torrent_name": torrent_name,
                        "rel_folder": f["folder"],
                        "rel_path": f["rel_path"],
                        "torrent_filename": f["filename"],
                        "torrent_cached_path": cached_meta_file,
                        "debrid_provider": str(debrid_p),
                        "debrid_key": (str(debrid_k) or "").strip(),
                        "download_connections": int(d_conn),
                        "parallel_workers": int(workers),
                        "res_preset": str(res_p),
                        "video_codec": str(v_cod),
                        "container_format": str(cont).lower(),
                        "audio_preset": str(aud_p),
                        "crf_val": int(crf_v),
                        "status": "Queued for Download",
                        "download_pct": 0.0,
                        "download_speed": "Queued...",
                        "downloaded_str": "0 MB",
                        "total_str": f["size_str"],
                        "convert_pct": 0.0,
                        "raw_size_mb": 0.0,
                        "final_size_mb": 0.0,
                        "chunk_status": {},
                        "error_msg": "",
                        "cancelled": False,
                        "active_proc": None,
                        "active_procs": []
                    }
                    config.tasks.append(new_task)
                    threading.Thread(target=run_task_pipeline, args=(new_task,), daemon=True).start()

            msg = f"✅ Queued {queued_count} video(s) from **{torrent_name}**!"
            return gr.update(visible=True, value=msg), render_dashboard_html(), gr.Tabs(selected="tab_downloads")

        btn_queue_torrent_videos.click(
            fn=handle_queue_torrent_videos,
            inputs=[
                torrent_videos_checkbox, torrent_meta_state,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                workers_slider, crf_slider
            ],
            outputs=[torrent_action_msg, dashboard_component, main_tabs]
        )

        # -------------------------------------------------------------
        # Search & Discovery Handlers
        # -------------------------------------------------------------
        def handle_search(query, media_type):
            choices, cache, msg = search_media_imdb(query, media_type)
            if not choices:
                return (
                    gr.update(visible=False, choices=[], value=None),
                    gr.update(visible=False),
                    gr.update(visible=False),
                    gr.update(visible=False),
                    cache, {}, {},
                    msg
                )
            first_label = choices[0]
            first_info = cache[first_label]
            is_tv = first_info['type'] == 'tv'

            m_panel_vis = not is_tv
            t_panel_vis = is_tv

            m_poster, m_details = (first_info.get('poster'), f"### {first_info['title']} ({first_info['year']})\nStars: {first_info['stars']}") if not is_tv else (None, "")

            t_seasons_data = {}
            t_season_choices = []
            t_ep_choices = []
            t_poster, t_details = (None, "")

            if is_tv:
                t_poster = first_info.get('poster')
                t_details = f"### {first_info['title']} ({first_info['year']})\nStars: {first_info['stars']}"
                t_seasons_data = get_tv_seasons_data(first_info['id'])
                if t_seasons_data:
                    t_season_choices = [f"Season {s}" for s in sorted(t_seasons_data.keys())]
                    first_season_num = sorted(t_seasons_data.keys())[0]
                    t_ep_choices = [e['label'] for e in t_seasons_data[first_season_num]]

            # Fetch multi-tracker live torrent swarms
            s_val = t_season_choices[0] if is_tv and t_season_choices else None
            t_choices, t_map = search_live_torrents(first_info['title'], year=first_info.get('year'), mode='tv' if is_tv else 'movie', season=s_val)

            return (
                gr.update(visible=True, choices=choices, value=first_label),
                gr.update(visible=m_panel_vis),
                gr.update(visible=t_panel_vis),
                gr.update(visible=True),
                cache,
                first_info,
                t_seasons_data,
                msg,
                m_poster, m_details,
                t_poster, t_details,
                gr.update(choices=t_season_choices, value=t_season_choices[0] if t_season_choices else None),
                gr.update(choices=t_ep_choices, value=t_ep_choices),
                gr.update(choices=t_choices, value=t_choices[0] if t_choices else None),
                t_map
            )

        btn_search.click(
            fn=handle_search,
            inputs=[search_input, media_type_filter],
            outputs=[
                media_select_dropdown, movie_panel, tv_panel, swarm_panel,
                search_cache, selected_media_cache, tv_seasons_cache, search_status,
                movie_poster, movie_details, tv_poster, tv_details,
                tv_season_select, tv_episodes_checkbox, live_torrents_dropdown, live_torrents_cache
            ]
        )
        search_input.submit(
            fn=handle_search,
            inputs=[search_input, media_type_filter],
            outputs=[
                media_select_dropdown, movie_panel, tv_panel, swarm_panel,
                search_cache, selected_media_cache, tv_seasons_cache, search_status,
                movie_poster, movie_details, tv_poster, tv_details,
                tv_season_select, tv_episodes_checkbox, live_torrents_dropdown, live_torrents_cache
            ]
        )

        def handle_media_select(selected_label, cache):
            if not selected_label or selected_label not in cache:
                return (
                    gr.update(visible=False), gr.update(visible=False), gr.update(visible=False),
                    {}, None, "", None, "", gr.update(choices=[]), gr.update(choices=[]),
                    gr.update(choices=[]), {}
                )
            info = cache[selected_label]
            is_tv = info['type'] == 'tv'

            m_poster, m_details = (info.get('poster'), f"### {info['title']} ({info['year']})\nStars: {info['stars']}") if not is_tv else (None, "")
            t_poster, t_details = (info.get('poster'), f"### {info['title']} ({info['year']})\nStars: {info['stars']}") if is_tv else (None, "")

            t_seasons_data = {}
            t_season_choices = []
            t_ep_choices = []
            if is_tv:
                t_seasons_data = get_tv_seasons_data(info['id'])
                if t_seasons_data:
                    t_season_choices = [f"Season {s}" for s in sorted(t_seasons_data.keys())]
                    first_season_num = sorted(t_seasons_data.keys())[0]
                    t_ep_choices = [e['label'] for e in t_seasons_data[first_season_num]]

            s_val = t_season_choices[0] if is_tv and t_season_choices else None
            t_choices, t_map = search_live_torrents(info['title'], year=info.get('year'), mode='tv' if is_tv else 'movie', season=s_val)

            return (
                gr.update(visible=not is_tv),
                gr.update(visible=is_tv),
                gr.update(visible=True),
                info,
                t_seasons_data,
                m_poster, m_details,
                t_poster, t_details,
                gr.update(choices=t_season_choices, value=t_season_choices[0] if t_season_choices else None),
                gr.update(choices=t_ep_choices, value=t_ep_choices),
                gr.update(choices=t_choices, value=t_choices[0] if t_choices else None),
                t_map
            )

        media_select_dropdown.change(
            fn=handle_media_select,
            inputs=[media_select_dropdown, search_cache],
            outputs=[
                movie_panel, tv_panel, swarm_panel, selected_media_cache, tv_seasons_cache,
                movie_poster, movie_details, tv_poster, tv_details,
                tv_season_select, tv_episodes_checkbox, live_torrents_dropdown, live_torrents_cache
            ]
        )

        def handle_season_change(season_label, seasons, info):
            if not season_label or not seasons:
                return gr.update(choices=[], value=[]), gr.update(choices=[]), {}
            try:
                s_num = int(re.search(r'\d+', season_label).group(0))
            except Exception:
                s_num = 1
            eps = seasons.get(s_num, [])
            ep_choices = [e['label'] for e in eps]

            title = info.get('title', '') if info else ''
            year = info.get('year', '') if info else ''
            t_choices, t_map = search_live_torrents(title, year=year, mode='tv', season=s_num)

            return (
                gr.update(choices=ep_choices, value=ep_choices),
                gr.update(choices=t_choices, value=t_choices[0] if t_choices else None),
                t_map
            )

        tv_season_select.change(
            fn=handle_season_change,
            inputs=[tv_season_select, tv_seasons_cache, selected_media_cache],
            outputs=[tv_episodes_checkbox, live_torrents_dropdown, live_torrents_cache]
        )

        btn_select_all_eps.click(
            fn=lambda s_label, seasons: gr.update(value=[e['label'] for e in seasons.get(int(re.search(r'\d+', s_label).group(0)), [])]) if s_label and seasons else gr.update(value=[]),
            inputs=[tv_season_select, tv_seasons_cache],
            outputs=[tv_episodes_checkbox]
        )
        btn_deselect_all_eps.click(
            fn=lambda: gr.update(value=[]),
            outputs=[tv_episodes_checkbox]
        )

        # Queue Movie Handler
        def handle_queue_movie(info, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not info:
                return "⚠️ No movie selected.", render_dashboard_html(), gr.update()
            imdb_id = info.get('id')
            title = info.get('title', 'Movie')
            year = info.get('year')
            full_title = f"{title} ({year})" if year else title
            url = f"https://videm.xyz/embed/movie/{imdb_id}"
            count = queue_direct_items(
                [(url, full_title)],
                workers_val=int(workers),
                debrid_prov=str(debrid_p),
                debrid_k=str(debrid_k),
                download_conn=int(d_conn),
                res_preset=str(res_p),
                v_codec=str(v_cod),
                container=str(cont),
                audio_preset=str(aud_p),
                crf_val=int(crf_v)
            )
            return f"✅ Queued movie '{full_title}' for download & conversion!", render_dashboard_html(), gr.Tabs(selected="tab_downloads")

        btn_queue_movie.click(
            fn=handle_queue_movie,
            inputs=[
                selected_media_cache,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                workers_slider, crf_slider
            ],
            outputs=[search_action_msg, dashboard_component, main_tabs]
        )

        # Queue TV Episodes Handler
        def handle_queue_episodes(selected_eps, season_label, seasons, info, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not selected_eps or not season_label or not seasons or not info:
                return "⚠️ Please select at least one episode to queue.", render_dashboard_html(), gr.update()
            try:
                s_num = int(re.search(r'\d+', season_label).group(0))
            except Exception:
                s_num = 1
            show_name = info.get('title', 'TV Series')
            all_eps = seasons.get(s_num, [])
            ep_map = {e['label']: e for e in all_eps}

            items = []
            for label in selected_eps:
                e_data = ep_map.get(label)
                if e_data:
                    ep_num = e_data['episode']
                    ep_name = e_data['name']
                    custom_title = f"{show_name} S{s_num:02d}E{ep_num:02d} - {ep_name}"
                    items.append((e_data['embed_url'], custom_title))

            if not items:
                return "⚠️ No valid episodes matched.", render_dashboard_html(), gr.update()

            count = queue_direct_items(
                items,
                workers_val=int(workers),
                debrid_prov=str(debrid_p),
                debrid_k=str(debrid_k),
                download_conn=int(d_conn),
                res_preset=str(res_p),
                v_codec=str(v_cod),
                container=str(cont),
                audio_preset=str(aud_p),
                crf_val=int(crf_v)
            )
            return f"✅ Queued {count} episode(s) of '{show_name}' Season {s_num}!", render_dashboard_html(), gr.Tabs(selected="tab_downloads")

        btn_queue_selected_eps.click(
            fn=handle_queue_episodes,
            inputs=[
                tv_episodes_checkbox, tv_season_select, tv_seasons_cache, selected_media_cache,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                workers_slider, crf_slider
            ],
            outputs=[search_action_msg, dashboard_component, main_tabs]
        )

        # Quick Queue Torrent Swarm Handler
        def handle_queue_live_torrent(selected_label, t_map, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not selected_label or not t_map or selected_label not in t_map:
                return "⚠️ Please select a torrent from the list to queue.", render_dashboard_html(), gr.update()
            t_info = t_map[selected_label]
            mag = t_info["magnet"]
            title = sanitize_filename(t_info["title"])

            with tasks_lock:
                config.task_counter += 1
                new_task = {
                    "id": config.task_counter,
                    "url": mag,
                    "title": title,
                    "has_custom_title": True,
                    "is_torrent_file": False,
                    "torrent_name": title,
                    "rel_folder": "Root",
                    "debrid_provider": str(debrid_p),
                    "debrid_key": (str(debrid_k) or "").strip(),
                    "download_connections": int(d_conn),
                    "parallel_workers": int(workers),
                    "res_preset": str(res_p),
                    "video_codec": str(v_cod),
                    "container_format": str(cont).lower(),
                    "audio_preset": str(aud_p),
                    "crf_val": int(crf_v),
                    "status": "Queued for Download",
                    "download_pct": 0.0,
                    "download_speed": "Queued...",
                    "downloaded_str": "0 MB",
                    "total_str": t_info.get("size_str", "Unknown"),
                    "convert_pct": 0.0,
                    "raw_size_mb": 0.0,
                    "final_size_mb": 0.0,
                    "chunk_status": {},
                    "error_msg": "",
                    "cancelled": False,
                    "active_proc": None,
                    "active_procs": []
                }
                config.tasks.append(new_task)
                threading.Thread(target=run_task_pipeline, args=(new_task,), daemon=True).start()

            msg = f"✅ Queued torrent '{title}' ({t_info['size_str']}, {t_info['seeds']} seeds)!"
            return msg, render_dashboard_html(), gr.Tabs(selected="tab_downloads")

        btn_quick_queue_swarm.click(
            fn=handle_queue_live_torrent,
            inputs=[
                live_torrents_dropdown, live_torrents_cache,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                workers_slider, crf_slider
            ],
            outputs=[search_action_msg, dashboard_component, main_tabs]
        )

        # Send Torrent to Explorer Sub-Tab Handler
        def handle_send_torrent_to_explorer(selected_label, t_map):
            if not selected_label or not t_map or selected_label not in t_map:
                return (
                    gr.update(), gr.update(visible=False), "",
                    gr.update(choices=["All Folders"], value="All Folders"),
                    gr.update(choices=[], value=[]),
                    gr.update(visible=True, value="⚠️ Please select a torrent from the list first."),
                    {}, "⚠️ Please select a torrent from the list.",
                    gr.update()
                )
            t_info = t_map[selected_label]
            mag = t_info["magnet"]
            res = handle_inspect_torrent(mag, None)
            # Switch subtabs to subtab_torrent!
            return (
                (gr.update(value=mag),) + res +
                (f"🔍 Loaded '{t_info['title']}' into Torrent Explorer tab!", gr.Tabs(selected="subtab_torrent"))
            )

        btn_send_to_explorer.click(
            fn=handle_send_torrent_to_explorer,
            inputs=[live_torrents_dropdown, live_torrents_cache],
            outputs=[
                torrent_magnet_input, torrent_tree_group, torrent_summary_html,
                torrent_folder_filter, torrent_videos_checkbox, torrent_action_msg,
                torrent_meta_state, search_action_msg, fetch_subtabs
            ]
        )

        # Refresh Torrents in Search Tab
        btn_refresh_torrents.click(
            fn=lambda media, season: search_live_torrents(media.get('title', ''), year=media.get('year', ''), mode='tv' if media.get('type') == 'tv' else 'movie', season=int(re.search(r'\d+', season).group(0)) if season and re.search(r'\d+', season) else None) if media else ([], {}),
            inputs=[selected_media_cache, tv_season_select],
            outputs=[live_torrents_dropdown, live_torrents_cache]
        )

        # Copy Movie / Episodes to Direct Links box
        def handle_copy_movie_to_direct(info):
            if not info:
                return "", ""
            return f"https://videm.xyz/embed/movie/{info['id']}", f"{info['title']} ({info.get('year', '')})".strip()

        btn_insert_movie_direct.click(
            fn=handle_copy_movie_to_direct,
            inputs=[selected_media_cache],
            outputs=[direct_input_box, direct_name_box]
        )

        def handle_copy_eps_to_direct(selected_eps, season_label, seasons, info):
            if not selected_eps or not season_label or not seasons or not info:
                return "", ""
            s_num = int(re.search(r'\d+', season_label).group(0)) if re.search(r'\d+', season_label) else 1
            show_name = info.get('title', 'TV Series')
            all_eps = seasons.get(s_num, [])
            ep_map = {e['label']: e for e in all_eps}
            urls = []
            names = []
            for l in selected_eps:
                e = ep_map.get(l)
                if e:
                    urls.append(e['embed_url'])
                    names.append(f"{show_name} S{s_num:02d}E{e['episode']:02d} - {e['name']}")
            return "\n".join(urls), "\n".join(names)

        btn_insert_eps_direct.click(
            fn=handle_copy_eps_to_direct,
            inputs=[tv_episodes_checkbox, tv_season_select, tv_seasons_cache, selected_media_cache],
            outputs=[direct_input_box, direct_name_box]
        )

        # Connect Single Clear Completed Button
        btn_clear_completed.click(
            fn=lambda: (clear_done_tasks(), render_dashboard_html())[1],
            outputs=[dashboard_component]
        )

    # Attach FastAPI native Cancel & Task Endpoints to Gradio's underlying app (after Blocks context)
    @demo.app.post("/api/task/{task_id}/cancel")
    @demo.app.get("/api/task/{task_id}/cancel")
    def api_cancel_task(task_id: int):
        cancelled = cancel_task_by_id(task_id)
        return {"status": "ok", "task_id": task_id, "cancelled": cancelled}

    @demo.app.get("/api/tasks")
    def api_list_tasks():
        with tasks_lock:
            return {"tasks": list(config.tasks)}

    return demo
