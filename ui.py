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
        <div style="text-align: center; padding: 36px 20px; background: #16161e; border: 1px dashed #313244; border-radius: 12px; color: #7f849c;">
            <div style="font-size: 32px; margin-bottom: 8px;">🎬</div>
            <div style="font-size: 15px; font-weight: 600; color: #a6adc8;">No active or queued video processes</div>
            <div style="font-size: 13px; color: #6c7086; margin-top: 4px;">Search for a movie or TV show above, inspect a torrent, or paste links to start processing.</div>
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
            folder_badge = f'<span style="background: #313244; color: #a6adc8; border: 1px solid #45475a; font-size: 11px; padding: 2px 8px; border-radius: 6px; margin-left: 8px;">📁 {html.escape(task["rel_folder"])}</span>'

        card = [f"""
        <div style="background: #1e1e2e; border: 1px solid {sc['border']}; border-radius: 12px; padding: 16px 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.25);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                <div style="font-weight: 600; font-size: 15px; color: #cdd6f4; max-width: 70%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                    🎬 #{task_id}: {title}{folder_badge}
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="background: {sc['bg']}; color: {sc['fg']}; border: 1px solid {sc['border']}; font-weight: 600; font-size: 12px; padding: 4px 12px; border-radius: 20px;">
                        {st}
                    </span>
                    <button id="cancel-btn-{task_id}"
                            onclick="window.cancelTask({task_id})"
                            title="Cancel process #{task_id} and delete all related files"
                            style="background: rgba(243, 139, 168, 0.15); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.4); font-weight: 600; font-size: 11px; padding: 4px 10px; border-radius: 6px; cursor: pointer; transition: all 0.2s ease;">
                        ✕ Cancel
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
                <span>Transcoding to 480p MKV: <b>{overall_pct:.1f}%</b> ({curr_str} / {dur_str})</span>
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
                <span>480p MKV: <b>{final_mb:.1f} MB</b></span>
                <span style="color: #a6e3a1; font-weight: 600;">⚡ Compressed: <b>{saved_pct:.1f}% space saved</b></span>
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

# Custom Global JavaScript injected into Gradio's Head
HEAD_JS = """
<script>
window.cancelTask = function(taskId) {
    var btn = document.getElementById('cancel-btn-' + taskId);
    if (btn) {
        btn.innerText = 'Cancelling...';
        btn.style.opacity = '0.5';
        btn.disabled = true;
    }
    fetch('/api/task/' + taskId + '/cancel', { method: 'POST' })
        .then(function(res) { return res.json(); })
        .then(function(data) {
            console.log('Task cancelled:', data);
        })
        .catch(function(err) {
            console.error('Cancel error:', err);
        });
};
</script>
<style>
/* Modern Sleek Dark Mode Overrides */
.tabs { border-bottom: 2px solid #313244 !important; }
.tab-nav button.selected { border-bottom: 2px solid #cba6f7 !important; color: #cba6f7 !important; font-weight: 700 !important; }
</style>
"""

def create_ui() -> gr.Blocks:
    with gr.Blocks(title="🎬 Colab Video Downloader & 480p MKV Engine", head=HEAD_JS, theme=gr.themes.Soft(primary_hue="purple", neutral_hue="slate")) as demo:
        gr.Markdown("""
        # 🎬 Colab Video Downloader & 480p Ultra-Compact MKV Batch Engine
        High-performance asynchronous video pipeline running inside Google Colab with hardware NVENC GPU encoding and Google Drive preservation.
        """)

        with gr.Tabs() as main_tabs:
            # -------------------------------------------------------------
            # TAB 1: 🔎 Instant Title Search & Episode Discovery
            # -------------------------------------------------------------
            with gr.Tab("🔎 Search Titles (Movies & TV Series)", id="tab_search"):
                with gr.Row():
                    search_input = gr.Textbox(
                        label="Search Title",
                        placeholder="Search movie or series name (e.g. FBI, Inception, Breaking Bad, Reacher)...",
                        scale=4,
                        lines=1
                    )
                    search_filter = gr.Radio(
                        choices=["All", "🎬 Movies", "📺 TV Series"],
                        value="All",
                        label="Filter",
                        scale=2
                    )
                    search_btn = gr.Button("🔍 Search", variant="primary", scale=1)

                search_status = gr.Markdown("", visible=False)
                search_results_dropdown = gr.Dropdown(
                    label="📋 Search Results (Click to inspect or download)",
                    choices=[],
                    visible=False
                )

                with gr.Column(visible=False) as details_container:
                    media_header_md = gr.HTML("")

                    # Movie Action Row
                    with gr.Row(visible=False) as movie_row:
                        btn_queue_movie = gr.Button("📥 Add Movie to Download Queue", variant="primary", scale=2)

                    # TV Series Section
                    with gr.Column(visible=False) as tv_col:
                        with gr.Row():
                            tv_season_select = gr.Dropdown(label="Select Season", choices=[], scale=2)
                            btn_select_all_eps = gr.Button("✅ Select All Season Episodes", scale=1)
                            btn_deselect_all_eps = gr.Button("◻️ Deselect All", scale=1)
                        tv_episodes_checkbox = gr.CheckboxGroup(label="Available Episodes (Check episodes to download)", choices=[], value=[])
                        with gr.Row():
                            btn_queue_episodes = gr.Button("📥 Add Selected Episode(s) to Queue", variant="primary", scale=2)

                    # Live Multi-Source Torrents Section
                    with gr.Column(visible=True) as torrents_live_col:
                        gr.Markdown("#### 🧲 Available Live Torrents & Packs (Multi-Source Swarms: TPB, YTS, 1337x, etc.)")
                        with gr.Row():
                            live_torrents_dropdown = gr.Dropdown(
                                label="Available Torrents (Filtered by Quality & Active Seeds)",
                                choices=[],
                                value=None,
                                scale=4
                            )
                            btn_refresh_torrents = gr.Button("🔄 Refresh Torrents", scale=1)
                        with gr.Row():
                            btn_send_to_explorer = gr.Button("🔍 Inspect in Torrent Explorer (Select Custom Episodes)", variant="secondary", scale=2)
                            btn_queue_live_torrent = gr.Button("📥 1-Click Queue Torrent", variant="primary", scale=2)

                search_action_msg = gr.Markdown("", visible=False)

            # -------------------------------------------------------------
            # TAB 2: 🧲 Torrent & Magnet File Explorer (Single Unified Place!)
            # -------------------------------------------------------------
            with gr.Tab("🧲 Torrent & Magnet Explorer", id="tab_torrent"):
                gr.Markdown("Inspect torrent contents, filter by folder (e.g. `Season 01`), and cherry-pick specific videos before downloading.")
                with gr.Row():
                    torrent_input_magnet = gr.Textbox(
                        label="Magnet Link",
                        placeholder="Paste magnet:?xt=urn:btih:... here to inspect contents...",
                        scale=4,
                        lines=1
                    )
                    torrent_input_file = gr.File(
                        label="Or Upload .torrent File",
                        file_count="single",
                        file_types=[".torrent"],
                        scale=2
                    )
                    btn_inspect_torrent = gr.Button("🔍 Inspect Torrent Files", variant="primary", scale=1)

                torrent_inspect_status = gr.Markdown("", visible=False)

                with gr.Column(visible=False) as torrent_details_container:
                    torrent_header_md = gr.HTML("")

                    with gr.Row():
                        torrent_folder_dropdown = gr.Dropdown(
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
                        btn_queue_torrent_videos = gr.Button("📥 Add Selected Video(s) to Queue", variant="primary", scale=3)

                torrent_action_msg = gr.Markdown("", visible=False)

            # -------------------------------------------------------------
            # TAB 3: 🔗 Direct Video Links & Web Streams
            # -------------------------------------------------------------
            with gr.Tab("🔗 Direct Links & Custom Streams", id="tab_direct"):
                with gr.Row():
                    with gr.Column(scale=4):
                        direct_input_box = gr.Textbox(
                            label="🔗 Video / Embed Links (Comma or newline separated)",
                            placeholder="https://example.com/video.mp4\nhttps://videm.xyz/embed/movie/tt1375666\nhttps://vidsrc.mov/embed/tv/80748/1/1",
                            lines=4
                        )
                        direct_name_box = gr.Textbox(
                            label="🏷️ Custom File Name(s) (Optional)",
                            placeholder="e.g. Inception (2010) or Part1, Part2 (leave empty to auto-detect original title)",
                            lines=1
                        )
                    with gr.Column(scale=1):
                        btn_add_direct = gr.Button("➕ Add to Queue", variant="primary", size="lg")

            # -------------------------------------------------------------
            # Global Engine & Hardware Settings (Collapsible)
            # -------------------------------------------------------------
            with gr.Accordion("⚙️ Torrent Engine & Hardware Settings", open=False):
                with gr.Row():
                    debrid_provider = gr.Dropdown(
                        choices=["Local aria2c (Encrypted / Download-Only)", "TorBox (Free / Paid)", "Real-Debrid"],
                        value="Local aria2c (Encrypted / Download-Only)",
                        label="Torrent Engine / Mode",
                        scale=2
                    )
                    debrid_api_key = gr.Textbox(
                        label="Cloud API Key (Optional for Local mode)",
                        placeholder="Paste TorBox/Real-Debrid key only if using cloud bridge",
                        type="password",
                        scale=2
                    )
                    workers_slider = gr.Slider(
                        label="Parallel Chunks per Video",
                        minimum=2,
                        maximum=4,
                        value=3,
                        step=1,
                        scale=1
                    )

        # -------------------------------------------------------------
        # Live Processes Dashboard (Single Clear Completed Button!)
        # -------------------------------------------------------------
        with gr.Row(variant="panel"):
            with gr.Column(scale=4):
                gr.Markdown("### 📋 Processes Panel (Max 3 Concurrent Downloads | 1 Dedicated NVENC Transcoder)")
            with gr.Column(scale=1, min_width=160):
                btn_clear_completed = gr.Button("🧹 Clear Completed", variant="secondary", size="sm")

        dashboard_component = gr.HTML(value=render_dashboard_html)

        # Reactive 1-Second Timer for real-time progress updates
        timer = gr.Timer(value=1.0)
        timer.tick(fn=render_dashboard_html, outputs=dashboard_component)

        # Persistent States
        search_cache_state = gr.State({})
        tv_seasons_state = gr.State({})
        current_media_state = gr.State({})
        live_torrents_state = gr.State({})
        torrent_meta_state = gr.State({})

        # -------------------------------------------------------------
        # Event Handlers
        # -------------------------------------------------------------
        def handle_search(query, media_type):
            choices, cache, msg = search_media_imdb(query, media_type)
            if not choices:
                return gr.Dropdown(choices=[], value=None, visible=False), gr.Markdown(f"⚠️ {msg}", visible=True), gr.Column(visible=False), cache
            return gr.Dropdown(choices=choices, value=None, visible=True), gr.Markdown(f"✅ {msg} Select a title from the list below:", visible=True), gr.Column(visible=False), cache

        def handle_select_media(selected_label, cache):
            if not selected_label or selected_label not in cache:
                return (
                    gr.Column(visible=False), "", gr.Row(visible=False), gr.Column(visible=False),
                    gr.Dropdown(choices=[]), gr.CheckboxGroup(choices=[], value=[]), {}, {},
                    gr.Dropdown(choices=[], value=None), {}
                )
            info = cache[selected_label]
            title = info.get('title', 'Unknown')
            year = info.get('year', '')
            stars = info.get('stars', '')
            is_tv = info.get('type') == 'tv'
            imdb_id = info.get('id', '')
            poster = info.get('poster')

            poster_html = f'<img src="{poster}" style="height: 110px; border-radius: 6px; float: left; margin-right: 15px; box-shadow: 0 4px 10px rgba(0,0,0,0.4);">' if poster else ''
            type_str = '📺 TV Series' if is_tv else '🎬 Movie'
            meta_html = f"""
            <div style="background: #1e1e2e; border: 1px solid #313244; padding: 12px 16px; border-radius: 8px; margin-bottom: 10px; overflow: hidden;">
                {poster_html}
                <div style="overflow: hidden;">
                    <div style="font-size: 17px; font-weight: 700; color: #cdd6f4;">{type_str}: {title} {f'({year})' if year else ''}</div>
                    <div style="font-size: 13px; color: #a6adc8; margin-top: 4px;"><b>IMDb ID:</b> <code style="color: #89b4fa;">{imdb_id}</code> {f'| <b>Cast:</b> {stars}' if stars else ''}</div>
                    <div style="font-size: 12px; color: #a6e3a1; margin-top: 6px;">⚡ Streaming Source: <b>Videm High-Speed Server Cluster</b> | 🧲 Torrent Indexer: <b>Live Multi-Tracker Swarm</b></div>
                </div>
            </div>
            """

            if not is_tv:
                t_choices, t_map = search_live_torrents(title, year=year, mode="movie")
                t_val = t_choices[0] if t_choices else None
                return (
                    gr.Column(visible=True), meta_html, gr.Row(visible=True), gr.Column(visible=False),
                    gr.Dropdown(choices=[]), gr.CheckboxGroup(choices=[], value=[]), info, {},
                    gr.Dropdown(choices=t_choices, value=t_val), t_map
                )
            else:
                show_name, seasons = get_tv_seasons_data(imdb_id)
                if not seasons:
                    seasons = {1: [
                        {'season': 1, 'episode': ep, 'name': f"Episode {ep}", 'embed_url': f"https://videm.xyz/embed/tv/{imdb_id}/1/{ep}", 'label': f"EP {ep:02d}"}
                        for ep in range(1, 26)
                    ]}
                season_choices = [f"Season {s}" for s in sorted(seasons.keys())]
                first_s = sorted(seasons.keys())[0] if seasons else 1
                eps = seasons.get(first_s, [])
                ep_choices = [e['label'] for e in eps]

                t_choices, t_map = search_live_torrents(title, year=year, mode="tv", season=first_s)
                t_val = t_choices[0] if t_choices else None

                return (
                    gr.Column(visible=True), meta_html, gr.Row(visible=False), gr.Column(visible=True),
                    gr.Dropdown(choices=season_choices, value=f"Season {first_s}"),
                    gr.CheckboxGroup(choices=ep_choices, value=[]), info, seasons,
                    gr.Dropdown(choices=t_choices, value=t_val), t_map
                )

        def handle_season_change(season_label, seasons, current_media):
            if not season_label or not seasons:
                return gr.CheckboxGroup(choices=[], value=[]), gr.Dropdown(choices=[], value=None), {}
            try:
                s_num = int(re.search(r'\d+', season_label).group(0))
            except Exception:
                s_num = 1
            eps = seasons.get(s_num, [])
            ep_choices = [e['label'] for e in eps]

            title = current_media.get('title', '') if current_media else ''
            year = current_media.get('year', '') if current_media else ''
            t_choices, t_map = search_live_torrents(title, year=year, mode="tv", season=s_num) if title else ([], {})
            t_val = t_choices[0] if t_choices else None
            return gr.CheckboxGroup(choices=ep_choices, value=[]), gr.Dropdown(choices=t_choices, value=t_val), t_map

        def handle_select_all(season_label, seasons):
            if not season_label or not seasons:
                return gr.CheckboxGroup(value=[])
            try:
                s_num = int(re.search(r'\d+', season_label).group(0))
            except Exception:
                s_num = 1
            eps = seasons.get(s_num, [])
            return gr.CheckboxGroup(value=[e['label'] for e in eps])

        def handle_deselect_all():
            return gr.CheckboxGroup(value=[])

        def handle_queue_movie(info, workers):
            if not info:
                return "⚠️ No movie selected.", render_dashboard_html()
            imdb_id = info.get('id')
            title = info.get('title', 'Movie')
            year = info.get('year')
            full_title = f"{title} ({year})" if year else title
            url = f"https://videm.xyz/embed/movie/{imdb_id}"
            count = queue_direct_items([(url, full_title)], workers)
            return f"✅ Queued movie '{full_title}' for download & 480p conversion!", render_dashboard_html()

        def handle_queue_episodes(selected_eps, season_label, seasons, info, workers):
            if not selected_eps or not season_label or not seasons or not info:
                return "⚠️ Please select at least one episode to queue.", render_dashboard_html()
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
                return "⚠️ No valid episodes matched.", render_dashboard_html()

            count = queue_direct_items(items, workers)
            return f"✅ Queued {count} episode(s) of '{show_name}' Season {s_num} for download & 480p conversion!", render_dashboard_html()

        def handle_refresh_live_torrents(current_media, season_label):
            if not current_media:
                return gr.Dropdown(choices=[], value=None), {}
            title = current_media.get('title', '')
            year = current_media.get('year', '')
            is_tv = current_media.get('type') == 'tv'
            s_num = None
            if is_tv and season_label:
                try:
                    s_num = int(re.search(r'\d+', season_label).group(0))
                except Exception:
                    s_num = 1
            t_choices, t_map = search_live_torrents(title, year=year, mode='tv' if is_tv else 'movie', season=s_num)
            t_val = t_choices[0] if t_choices else None
            return gr.Dropdown(choices=t_choices, value=t_val), t_map

        def handle_send_torrent_to_explorer(selected_label, t_map):
            if not selected_label or not t_map or selected_label not in t_map:
                return (
                    gr.update(), gr.update(visible=False), "",
                    gr.update(choices=["All Folders"], value="All Folders"),
                    gr.update(choices=[], value=[]),
                    gr.update(visible=True, value="⚠️ Please select a torrent from the list first."),
                    {}, "⚠️ Please select a torrent from the list."
                )
            t_info = t_map[selected_label]
            mag = t_info["magnet"]
            res = handle_inspect_torrent(mag, None)
            return (gr.update(value=mag),) + res + (f"🔍 Loaded '{t_info['title']}' into Torrent Explorer tab! Switch to the 🧲 Torrent & Magnet Explorer tab to choose custom episodes.",)

        def handle_queue_live_torrent(selected_label, t_map, workers, debrid_prov="Local aria2c (Encrypted / Download-Only)", debrid_k=""):
            if not selected_label or not t_map or selected_label not in t_map:
                return "⚠️ Please select a torrent from the list to queue.", render_dashboard_html()
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
                    "debrid_provider": str(debrid_prov),
                    "debrid_key": (str(debrid_k) or "").strip(),
                    "status": "Queued for Download",
                    "parallel_workers": int(workers),
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
                    "active_proc": None
                }
                config.tasks.append(new_task)
                threading.Thread(target=run_task_pipeline, args=(new_task,), daemon=True).start()

            msg = f"✅ Queued torrent '{title}' ({t_info['size_str']}, {t_info['seeds']} seeds) for download & 480p conversion!"
            return msg, render_dashboard_html()

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

        def handle_select_all_torrent_videos(meta):
            if not meta or 'files' not in meta:
                return gr.update(value=[])
            return gr.update(value=[f['label'] for f in meta['files'] if f['is_video']])

        def handle_deselect_all_torrent_videos():
            return gr.update(value=[])

        def handle_queue_torrent_videos(selected_labels, meta, workers, debrid_prov="Local aria2c (Encrypted / Download-Only)", debrid_k=""):
            if not selected_labels or not meta or 'files' not in meta:
                return gr.update(visible=True, value="⚠️ Please select at least one video file from the list to download."), render_dashboard_html()

            files = meta['files']
            label_to_file = {f['label']: f for f in files if f['is_video']}
            torrent_name = meta['name']
            cached_meta_file = meta.get('cached_file_path')
            source_url = meta.get('source_url', '')

            items_to_queue = []
            for label in selected_labels:
                f = label_to_file.get(label)
                if f:
                    items_to_queue.append(f)

            if not items_to_queue:
                return gr.update(visible=True, value="⚠️ No valid video files selected."), render_dashboard_html()

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
                        "debrid_provider": str(debrid_prov),
                        "debrid_key": (str(debrid_k) or "").strip(),
                        "status": "Queued for Download",
                        "parallel_workers": int(workers),
                        "download_pct": 0.0,
                        "download_speed": "Queued...",
                        "downloaded_str": "0 MB",
                        "total_str": f['size_str'],
                        "convert_pct": 0.0,
                        "raw_size_mb": 0.0,
                        "final_size_mb": 0.0,
                        "chunk_status": {},
                        "error_msg": "",
                        "cancelled": False,
                        "active_proc": None
                    }
                    config.tasks.append(new_task)
                    threading.Thread(target=run_task_pipeline, args=(new_task,), daemon=True).start()

            msg = f"✅ Queued {queued_count} video(s) from **'{torrent_name}'**! They will transcode to 480p MKV and save into Google Drive: `/content/drive/MyDrive/vid/{sanitize_filename(torrent_name)}/<folder>/`."
            return gr.update(visible=True, value=msg), render_dashboard_html()

        def handle_add_direct(links_text, custom_names, workers, debrid_prov, debrid_k):
            raw_links = [l.strip() for l in re.split(r'[\r\n,]+', links_text or "") if l.strip()]
            raw_names = [n.strip() for n in re.split(r'[\r\n,]+', custom_names or "") if n.strip()]
            if not raw_links:
                return links_text, custom_names, render_dashboard_html()

            items = []
            for idx, url in enumerate(raw_links):
                c_name = raw_names[idx] if idx < len(raw_names) else ""
                items.append((url, c_name or f"Video_{idx+1}"))

            queue_direct_items(items, workers, debrid_prov, debrid_k)
            return "", "", render_dashboard_html()

        # Connect Search Events
        search_btn.click(
            fn=handle_search,
            inputs=[search_input, search_filter],
            outputs=[search_results_dropdown, search_status, details_container, search_cache_state]
        )
        search_input.submit(
            fn=handle_search,
            inputs=[search_input, search_filter],
            outputs=[search_results_dropdown, search_status, details_container, search_cache_state]
        )
        search_results_dropdown.change(
            fn=handle_select_media,
            inputs=[search_results_dropdown, search_cache_state],
            outputs=[details_container, media_header_md, movie_row, tv_col, tv_season_select, tv_episodes_checkbox, current_media_state, tv_seasons_state, live_torrents_dropdown, live_torrents_state]
        )
        tv_season_select.change(
            fn=handle_season_change,
            inputs=[tv_season_select, tv_seasons_state, current_media_state],
            outputs=[tv_episodes_checkbox, live_torrents_dropdown, live_torrents_state]
        )
        btn_select_all_eps.click(
            fn=handle_select_all,
            inputs=[tv_season_select, tv_seasons_state],
            outputs=[tv_episodes_checkbox]
        )
        btn_deselect_all_eps.click(
            fn=handle_deselect_all,
            outputs=[tv_episodes_checkbox]
        )
        btn_queue_movie.click(
            fn=handle_queue_movie,
            inputs=[current_media_state, workers_slider],
            outputs=[search_action_msg, dashboard_component]
        )
        btn_queue_episodes.click(
            fn=handle_queue_episodes,
            inputs=[tv_episodes_checkbox, tv_season_select, tv_seasons_state, current_media_state, workers_slider],
            outputs=[search_action_msg, dashboard_component]
        )
        btn_refresh_torrents.click(
            fn=handle_refresh_live_torrents,
            inputs=[current_media_state, tv_season_select],
            outputs=[live_torrents_dropdown, live_torrents_state]
        )
        btn_send_to_explorer.click(
            fn=handle_send_torrent_to_explorer,
            inputs=[live_torrents_dropdown, live_torrents_state],
            outputs=[torrent_input_magnet, torrent_details_container, torrent_header_md, torrent_folder_dropdown, torrent_videos_checkbox, torrent_inspect_status, torrent_meta_state, search_action_msg]
        )
        btn_queue_live_torrent.click(
            fn=handle_queue_live_torrent,
            inputs=[live_torrents_dropdown, live_torrents_state, workers_slider, debrid_provider, debrid_api_key],
            outputs=[search_action_msg, dashboard_component]
        )

        # Connect Torrent Explorer Events
        btn_inspect_torrent.click(
            fn=handle_inspect_torrent,
            inputs=[torrent_input_magnet, torrent_input_file],
            outputs=[torrent_details_container, torrent_header_md, torrent_folder_dropdown, torrent_videos_checkbox, torrent_inspect_status, torrent_meta_state]
        )
        torrent_input_magnet.submit(
            fn=handle_inspect_torrent,
            inputs=[torrent_input_magnet, torrent_input_file],
            outputs=[torrent_details_container, torrent_header_md, torrent_folder_dropdown, torrent_videos_checkbox, torrent_inspect_status, torrent_meta_state]
        )
        torrent_input_file.change(
            fn=handle_inspect_torrent,
            inputs=[torrent_input_magnet, torrent_input_file],
            outputs=[torrent_details_container, torrent_header_md, torrent_folder_dropdown, torrent_videos_checkbox, torrent_inspect_status, torrent_meta_state]
        )
        torrent_folder_dropdown.change(
            fn=handle_torrent_folder_change,
            inputs=[torrent_folder_dropdown, torrent_meta_state, torrent_videos_checkbox],
            outputs=[torrent_videos_checkbox]
        )
        btn_select_entire_folder.click(
            fn=handle_select_entire_folder,
            inputs=[torrent_folder_dropdown, torrent_meta_state, torrent_videos_checkbox],
            outputs=[torrent_videos_checkbox]
        )
        btn_select_all_torrent.click(
            fn=handle_select_all_torrent_videos,
            inputs=[torrent_meta_state],
            outputs=[torrent_videos_checkbox]
        )
        btn_deselect_all_torrent.click(
            fn=handle_deselect_all_torrent_videos,
            outputs=[torrent_videos_checkbox]
        )
        btn_queue_torrent_videos.click(
            fn=handle_queue_torrent_videos,
            inputs=[torrent_videos_checkbox, torrent_meta_state, workers_slider, debrid_provider, debrid_api_key],
            outputs=[torrent_action_msg, dashboard_component]
        )

        # Connect Direct Queue Event
        btn_add_direct.click(
            fn=handle_add_direct,
            inputs=[direct_input_box, direct_name_box, workers_slider, debrid_provider, debrid_api_key],
            outputs=[direct_input_box, direct_name_box, dashboard_component]
        )
        direct_input_box.submit(
            fn=handle_add_direct,
            inputs=[direct_input_box, direct_name_box, workers_slider, debrid_provider, debrid_api_key],
            outputs=[direct_input_box, direct_name_box, dashboard_component]
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
