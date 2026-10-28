import os
import re
import html
import time
import json
import urllib.parse
import warnings
from typing import Dict, Any, List, Tuple, Optional

# Suppress benign Gradio 6.0 deprecation/user warnings for Blocks constructor parameters
warnings.filterwarnings(
    "ignore",
    message=r".*parameter in the Blocks constructor will be removed in Gradio 6\.0.*"
)
warnings.filterwarnings(
    "ignore",
    message=r".*The parameters have been moved from the Blocks constructor to the launch\(\) method.*"
)

import gradio as gr
import config
from config import tasks, tasks_lock, sanitize_filename
import database
import metadata_manager
import classifier
from torrent_parser import format_torrent_size, fetch_torrent_metadata, extract_file_path
from torrent_search import search_live_torrents
from media_search import search_media_imdb, get_tv_seasons_data
from pipeline import (
    cancel_task_by_id, clear_done_tasks, queue_direct_items,
    _active_refine_tasks, _refine_lock,
    get_unqueued_raw_files, enqueue_unqueued_raw_file, delete_unqueued_raw_file
)
import cluster_manager
import threading

# ---------------------------------------------------------
# HTML Renderers for Queues & Catalog
# ---------------------------------------------------------

def render_download_queue_html() -> str:
    """Renders active downloads, pending download queue with reordering (⬆️/⬇️), speed lanes, and full persistent history."""
    db_items = database.get_download_queue()

    with tasks_lock:
        mem_tasks = {t.get("db_id", t.get("id")): t for t in config.tasks}

    high_active = []
    low_active = []
    high_pending = []
    low_pending = []
    history_items = []

    for item in db_items:
        st = item.get("status", "pending").lower()
        is_low = bool(item.get("is_low_speed", 0))
        if st in ["downloading", "staging"]:
            if is_low:
                low_active.append(item)
            else:
                high_active.append(item)
        elif st == "pending":
            if is_low:
                low_pending.append(item)
            else:
                high_pending.append(item)
        else:
            history_items.append(item)

    # Invert history order so nearest/most recent is at the top
    history_items = sorted(history_items, key=lambda x: x.get("id", 0), reverse=True)

    html_parts = ['<div style="display: flex; flex-direction: column; gap: 16px; font-family: system-ui, -apple-system, sans-serif;">']

    # Helper function to render active download card
    def render_active_card(it, is_low_lane=False):
        i_id = it["id"]
        m_task = mem_tasks.get(i_id, {})
        pct = m_task.get("download_pct", it.get("download_pct", 0.0))
        spd = m_task.get("download_speed", it.get("download_speed", "--"))
        dl_str = m_task.get("downloaded_str", "0 MB")
        tot_str = m_task.get("total_str", "--")
        st_text = m_task.get("status", it.get("status", "Downloading"))
        title = html.escape(it.get("title", "Download"))

        border_col = "#f9e2af" if is_low_lane else "#89b4fa"
        badge_bg = "rgba(249, 226, 175, 0.15)" if is_low_lane else "#1e3a5f"
        badge_col = "#f9e2af" if is_low_lane else "#89b4fa"
        lane_label = "🐢 Slow Lane (<100 KB/s)" if is_low_lane else "⚡ High-Speed"

        return f"""
        <div id="dl-card-{i_id}" style="background: #1e1e2e; border: 1px solid {border_col}; border-radius: 10px; padding: 14px 18px; box-shadow: 0 4px 10px rgba(0,0,0,0.3);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <div style="font-weight: 600; font-size: 14px; color: #cdd6f4; max-width: 65%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                    📥 #{i_id}: {title}
                </div>
                <div style="display: flex; align-items: center; gap: 6px;">
                    <span style="background: {badge_bg}; color: {badge_col}; border: 1px solid {border_col}; font-weight: 600; font-size: 10px; padding: 2px 8px; border-radius: 12px;">
                        {lane_label}
                    </span>
                    <span style="background: #181825; color: #bac2de; border: 1px solid #313244; font-weight: 600; font-size: 11px; padding: 2px 8px; border-radius: 12px;">
                        {st_text}
                    </span>
                    <button onclick="window.cancelTask('download_queue', {i_id})" title="Cancel download" style="background: rgba(243, 139, 168, 0.15); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.4); font-weight: 600; font-size: 11px; padding: 3px 8px; border-radius: 6px; cursor: pointer;">
                        ✕ Cancel
                    </button>
                </div>
            </div>
            <div style="background: #313244; border-radius: 6px; height: 10px; width: 100%; overflow: hidden; margin: 6px 0;">
                <div style="background: linear-gradient(90deg, {border_col}, #b4befe); height: 100%; width: {pct:.1f}%; transition: width 0.3s ease;"></div>
            </div>
            <div style="display: flex; justify-content: space-between; font-size: 12px; color: #a6adc8;">
                <span>Progress: <b>{pct:.1f}%</b> ({dl_str} / {tot_str})</span>
                <span>⚡ Speed: <b>{spd}</b></span>
            </div>
        </div>
        """

    # Helper function to render pending item row
    def render_pending_row(it, idx, is_low_lane=False):
        i_id = it["id"]
        title = html.escape(it.get("title", "Media"))
        src_type = it.get("source_type", "direct")
        retries = it.get("retry_count", 0)
        badge_icon = "🧲" if src_type == "torrent" else "🔗"

        retry_badge = ""
        if retries > 0:
            retry_badge = f'<span style="background: rgba(249, 226, 175, 0.15); color: #f9e2af; border: 1px solid rgba(249, 226, 175, 0.3); font-size: 10px; padding: 1px 6px; border-radius: 8px; font-weight: 600;">⚠️ Retry {retries}/3</span>'

        lane_switch_btn = f"""
        <button onclick="window.switchQueueSpeed('download_queue', {i_id})" title="Switch to High-Speed Fast Lane" style="background: #1e3a5f; color: #89b4fa; border: 1px solid #89b4fa; border-radius: 4px; padding: 3px 8px; font-size: 11px; cursor: pointer;">
            ⚡ Fast
        </button>
        """ if is_low_lane else f"""
        <button onclick="window.switchQueueSpeed('download_queue', {i_id})" title="Move to Low-Speed Slow Lane" style="background: #313244; color: #f9e2af; border: 1px solid #f9e2af; border-radius: 4px; padding: 3px 8px; font-size: 11px; cursor: pointer;">
            🐢 Slow
        </button>
        """

        return f"""
        <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 10px 14px; display: flex; justify-content: space-between; align-items: center;">
            <div style="display: flex; align-items: center; gap: 8px; max-width: 65%;">
                <span style="font-size: 12px; font-weight: 700; color: #585b70; width: 24px;">#{idx + 1}</span>
                <span style="font-size: 14px; color: #cdd6f4; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                    {badge_icon} {title}
                </span>
                {retry_badge}
            </div>
            <div style="display: flex; align-items: center; gap: 6px;">
                <button onclick="window.moveQueueItem('download_queue', {i_id}, 'up')" title="Move Up" style="background: #313244; color: #cdd6f4; border: 1px solid #45475a; border-radius: 4px; padding: 3px 8px; font-size: 11px; cursor: pointer;">
                    ⬆️ Up
                </button>
                <button onclick="window.moveQueueItem('download_queue', {i_id}, 'down')" title="Move Down" style="background: #313244; color: #cdd6f4; border: 1px solid #45475a; border-radius: 4px; padding: 3px 8px; font-size: 11px; cursor: pointer;">
                    ⬇️ Down
                </button>
                {lane_switch_btn}
                <button onclick="window.cancelTask('download_queue', {i_id})" title="Remove from queue" style="background: rgba(243, 139, 168, 0.1); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.3); border-radius: 4px; padding: 3px 8px; font-size: 11px; cursor: pointer;">
                    ✕
                </button>
            </div>
        </div>
        """

    # 1. High-Speed Ongoing Section
    html_parts.append('<div style="font-weight: 700; font-size: 15px; color: #89b4fa; display: flex; align-items: center; gap: 8px;">⚡ Ongoing High-Speed Downloads (' + str(len(high_active)) + '/' + str(config.MAX_DOWNLOAD_WORKERS) + ')</div>')
    if not high_active:
        html_parts.append("""
        <div style="background: #181825; border: 1px dashed #313244; border-radius: 10px; padding: 14px; text-align: center; color: #6c7086; font-size: 13px;">
            No high-speed downloads active. Fast slots are ready.
        </div>
        """)
    else:
        for it in high_active:
            html_parts.append(render_active_card(it, is_low_lane=False))

    # 2. High-Speed Upcoming Queue Section
    html_parts.append('<div style="font-weight: 700; font-size: 15px; color: #bac2de; display: flex; align-items: center; gap: 8px; margin-top: 8px;">📋 Upcoming High-Speed Queue (' + str(len(high_pending)) + ')</div>')
    if not high_pending:
        html_parts.append("""
        <div style="background: #181825; border: 1px dashed #313244; border-radius: 10px; padding: 12px; text-align: center; color: #6c7086; font-size: 13px;">
            High-speed queue is empty.
        </div>
        """)
    else:
        for idx, it in enumerate(high_pending):
            html_parts.append(render_pending_row(it, idx, is_low_lane=False))

    # 3. Low-Speed Trickle Queue Section (Dedicated slow lane - Unlimited concurrent capacity)
    total_low = len(low_active) + len(low_pending)
    html_parts.append('<div style="font-weight: 700; font-size: 15px; color: #f9e2af; display: flex; align-items: center; gap: 8px; margin-top: 14px;">🐢 Low-Speed Trickle Queue (<100 KB/s: ' + str(len(low_active)) + ' active [Unlimited Capacity], ' + str(len(low_pending)) + ' queued)</div>')
    if total_low == 0:
        html_parts.append("""
        <div style="background: #181825; border: 1px dashed #313244; border-radius: 10px; padding: 12px; text-align: center; color: #6c7086; font-size: 13px;">
            No low-speed downloads demoted. High-speed queue is running smoothly.
        </div>
        """)
    else:
        for it in low_active:
            html_parts.append(render_active_card(it, is_low_lane=True))
        for idx, it in enumerate(low_pending):
            html_parts.append(render_pending_row(it, idx, is_low_lane=True))

    # 4. Finished Downloads (History: Completed, Failed with reason, Cancelled with reason)
    if history_items:
        html_parts.append('<div style="font-weight: 700; font-size: 14px; color: #a6adc8; display: flex; align-items: center; gap: 8px; margin-top: 14px;">📜 Finished & Staged History (' + str(len(history_items)) + ')</div>')
        for it in history_items:
            i_id = it["id"]
            title = html.escape(it.get("title", "Download"))
            st = it.get("status", "completed").lower()
            err_msg = html.escape((it.get("error_msg") or "").strip())
            src_type = it.get("source_type", "direct")
            badge_icon = "🧲" if src_type == "torrent" else "🔗"

            if st in ["completed", "staged"]:
                badge_html = '<span style="background: rgba(166, 227, 161, 0.15); color: #a6e3a1; border: 1px solid rgba(166, 227, 161, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">✅ STAGED IN DRIVE</span>'
                callout_html = '<div style="font-size: 12px; color: #6c7086; margin-top: 4px;">Saved to Google Drive raw/ and queued for GPU refining.</div>'
            elif st == "failed":
                badge_html = '<span style="background: rgba(243, 139, 168, 0.15); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">❌ FAILED</span>'
                callout_html = f'<div style="margin-top: 6px; padding: 6px 10px; background: rgba(243, 139, 168, 0.1); border-left: 3px solid #f38ba8; border-radius: 4px; font-size: 12px; color: #f38ba8;">❌ Error Reason: {err_msg or "Download interrupted or network failure."}</div>'
            else:
                # Cancelled / Skipped
                badge_html = '<span style="background: rgba(249, 226, 175, 0.15); color: #f9e2af; border: 1px solid rgba(249, 226, 175, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">⚠️ CANCELLED</span>'
                callout_html = f'<div style="margin-top: 6px; padding: 6px 10px; background: rgba(249, 226, 175, 0.1); border-left: 3px solid #f9e2af; border-radius: 4px; font-size: 12px; color: #f9e2af;">⚠️ Reason: {err_msg or "Cancelled by user."}</div>'

            html_parts.append(f"""
            <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 10px 14px; margin-bottom: 4px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="font-size: 13px; color: #cdd6f4; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 75%;">
                        {badge_icon} #{i_id}: {title}
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        {badge_html}
                        <button onclick="window.dismissTask('download_queue', {i_id})" title="Dismiss from list" style="background: transparent; color: #6c7086; border: 1px solid #313244; border-radius: 4px; padding: 2px 6px; font-size: 10px; cursor: pointer;">
                            ✕
                        </button>
                    </div>
                </div>
                {callout_html}
            </div>
            """)

    html_parts.append('</div>')
    return "\n".join(html_parts)

    # 3. Finished Downloads (History: Completed, Failed with reason, Cancelled with reason)
    if history_items:
        html_parts.append('<div style="font-weight: 700; font-size: 14px; color: #a6adc8; display: flex; align-items: center; gap: 8px; margin-top: 14px;">📜 Finished & Staged History (' + str(len(history_items)) + ')</div>')
        for it in history_items:
            i_id = it["id"]
            title = html.escape(it.get("title", "Download"))
            st = it.get("status", "completed").lower()
            err_msg = html.escape((it.get("error_msg") or "").strip())
            src_type = it.get("source_type", "direct")
            badge_icon = "🧲" if src_type == "torrent" else "🔗"

            if st in ["completed", "staged"]:
                badge_html = '<span style="background: rgba(166, 227, 161, 0.15); color: #a6e3a1; border: 1px solid rgba(166, 227, 161, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">✅ STAGED IN DRIVE</span>'
                callout_html = '<div style="font-size: 12px; color: #6c7086; margin-top: 4px;">Saved to Google Drive raw/ and queued for GPU refining.</div>'
            elif st == "failed":
                badge_html = '<span style="background: rgba(243, 139, 168, 0.15); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">❌ FAILED</span>'
                callout_html = f'<div style="margin-top: 6px; padding: 6px 10px; background: rgba(243, 139, 168, 0.1); border-left: 3px solid #f38ba8; border-radius: 4px; font-size: 12px; color: #f38ba8;">❌ Error Reason: {err_msg or "Download interrupted or network failure."}</div>'
            else:
                # Cancelled / Skipped
                badge_html = '<span style="background: rgba(249, 226, 175, 0.15); color: #f9e2af; border: 1px solid rgba(249, 226, 175, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">⚠️ CANCELLED</span>'
                callout_html = f'<div style="margin-top: 6px; padding: 6px 10px; background: rgba(249, 226, 175, 0.1); border-left: 3px solid #f9e2af; border-radius: 4px; font-size: 12px; color: #f9e2af;">⚠️ Reason: {err_msg or "Cancelled by user."}</div>'

            html_parts.append(f"""
            <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 10px 14px; margin-bottom: 4px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="font-size: 13px; color: #cdd6f4; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 75%;">
                        {badge_icon} #{i_id}: {title}
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        {badge_html}
                        <button onclick="window.dismissTask('download_queue', {i_id})" title="Dismiss from list" style="background: transparent; color: #6c7086; border: 1px solid #313244; border-radius: 4px; padding: 2px 6px; font-size: 10px; cursor: pointer;">
                            ✕
                        </button>
                    </div>
                </div>
                {callout_html}
            </div>
            """)

    html_parts.append('</div>')
    return "\n".join(html_parts)

def render_refine_queue_html() -> str:
    """Renders persistent refining queue from queue.db, active GPU chunk progress, and full history."""
    db_items = database.get_refine_queue()

    with _refine_lock:
        active_mem = dict(_active_refine_tasks)

    active_jobs = [it for it in db_items if it.get("status") == "refining"]
    pending_jobs = [it for it in db_items if it.get("status") == "pending"]
    # Invert history order so nearest/most recent is at the top
    history_jobs = sorted([it for it in db_items if it.get("status") in ["completed", "failed", "cancelled"]], key=lambda x: x.get("id", 0), reverse=True)

    html_parts = ['<div style="display: flex; flex-direction: column; gap: 16px; font-family: system-ui, -apple-system, sans-serif;">']

    # 0. Un-queued Raw Files Alert & Interactive Action Section
    unqueued_files = get_unqueued_raw_files()
    if unqueued_files:
        html_parts.append(f"""
        <div style="background: rgba(249, 226, 175, 0.08); border: 1px solid rgba(249, 226, 175, 0.4); border-radius: 10px; padding: 14px 18px; margin-bottom: 4px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                <div style="font-weight: 700; font-size: 15px; color: #f9e2af; display: flex; align-items: center; gap: 8px;">
                    📁 Un-queued Raw Files Detected in Drive ({len(unqueued_files)})
                </div>
                <div style="font-size: 12px; color: #bac2de;">
                    Found in raw/ folder but not in refining queue. Choose an action for each:
                </div>
            </div>
        """)
        for item in unqueued_files:
            fname = html.escape(item["filename"])
            raw_fname = item["filename"]
            sz = item["formatted_size"]
            js_fname = json.dumps(raw_fname)
            html_parts.append(f"""
            <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 10px 14px; margin-bottom: 6px; display: flex; justify-content: space-between; align-items: center;">
                <div style="font-size: 13px; color: #cdd6f4; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 60%;">
                    🎬 {fname} <span style="font-size: 11px; color: #7f849c; margin-left: 6px;">({sz})</span>
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <button onclick='window.rawAction("raw_enqueue", {js_fname})' title="Add this video to the refining queue" style="background: #2a3a30; color: #a6e3a1; border: 1px solid #a6e3a1; border-radius: 6px; padding: 4px 10px; font-size: 11px; font-weight: 600; cursor: pointer;">
                        ➕ Add to Refine Queue
                    </button>
                    <button onclick='window.rawAction("raw_delete", {js_fname})' title="Permanently delete from Google Drive" style="background: #3a2228; color: #f38ba8; border: 1px solid #f38ba8; border-radius: 6px; padding: 4px 10px; font-size: 11px; font-weight: 600; cursor: pointer;">
                        🗑️ Delete
                    </button>
                </div>
            </div>
            """)
        html_parts.append('</div>')

    # 1. Active Transcoding Job Section
    html_parts.append('<div style="font-weight: 700; font-size: 15px; color: #cba6f7; display: flex; align-items: center; gap: 8px;">⚙️ Active Transcoding (' + str(len(active_jobs)) + ')</div>')
    if not active_jobs:
        html_parts.append("""
        <div style="background: #181825; border: 1px dashed #313244; border-radius: 10px; padding: 18px; text-align: center; color: #6c7086; font-size: 13px;">
            No video currently being refined. Next pending job in queue will start automatically.
        </div>
        """)
    else:
        for job in active_jobs:
            j_id = job["id"]
            m_task = active_mem.get(j_id, {})
            pct = m_task.get("convert_pct", job.get("convert_pct", 0.0))
            chunks = m_task.get("chunk_status") or job.get("chunk_status", {})
            if isinstance(chunks, str):
                try:
                    chunks = json.loads(chunks)
                except Exception:
                    chunks = {}
            if chunks:
                total_curr = sum(c.get("curr_sec", 0.0) for c in chunks.values())
                total_dur = sum(c.get("dur_sec", 1.0) for c in chunks.values())
                if total_dur > 0:
                    pct = max(pct, min(99.9, (total_curr / total_dur) * 100.0))

            cfg = job.get("refining_config", {})
            res_tag = cfg.get("res_preset", "480p").split()[0]
            v_codec = "H.265" if "265" in cfg.get("video_codec", "H.265") else "H.264"
            cont = cfg.get("container_format", "mkv").upper()
            title = html.escape(job.get("title", "Media"))

            chunks_done_label = ""
            if chunks:
                d_c = sum(1 for c in chunks.values() if c.get("status") == "Done")
                chunks_done_label = f"({d_c}/{len(chunks)} Chunks)"

            html_parts.append(f"""
            <div id="refine-card-{j_id}" style="background: #1e1e2e; border: 1px solid #cba6f7; border-radius: 10px; padding: 16px 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.3);">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                    <div style="font-weight: 600; font-size: 15px; color: #cdd6f4; max-width: 75%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                        🎬 #{j_id}: {title}
                        <span style="background: rgba(203, 166, 247, 0.15); color: #cba6f7; border: 1px solid rgba(203, 166, 247, 0.4); font-size: 11px; padding: 2px 8px; border-radius: 6px; margin-left: 8px;">
                            Target: {res_tag} {cont} • {v_codec}
                        </span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <span style="background: #38234a; color: #cba6f7; border: 1px solid #cba6f7; font-weight: 600; font-size: 11px; padding: 2px 10px; border-radius: 12px;">
                            Refining
                        </span>
                        <button onclick="window.cancelTask('refine_queue', {j_id})" title="Cancel and abort" style="background: rgba(243, 139, 168, 0.15); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.4); font-weight: 600; font-size: 11px; padding: 3px 8px; border-radius: 6px; cursor: pointer;">
                            ✕ Cancel
                        </button>
                    </div>
                </div>
                <div style="background: #313244; border-radius: 6px; height: 10px; width: 100%; overflow: hidden; margin: 6px 0;">
                    <div style="background: linear-gradient(90deg, #cba6f7, #f5c2e7); height: 100%; width: {pct:.1f}%; transition: width 0.3s ease;"></div>
                </div>
                <div style="display: flex; justify-content: space-between; font-size: 12px; color: #a6adc8; margin-bottom: 8px;">
                    <span>Transcoding progress: <b>{pct:.1f}%</b> {chunks_done_label}</span>
                    <span>Raw Size: <b>{job.get('raw_size_mb', 0):.1f} MB</b></span>
                </div>
            """)

            if chunks and len(chunks) > 0:
                total_c = len(chunks)
                done_c = sum(1 for c in chunks.values() if c.get("status") == "Done")
                active_c = sum(1 for c in chunks.values() if c.get("status") in ["Encoding", "Retrying (CPU)"])
                html_parts.append(f"""
                <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 10px 12px; margin-top: 8px;">
                    <div style="display: flex; justify-content: space-between; font-size: 11px; font-weight: 600; color: #9399b2; margin-bottom: 8px;">
                        <span>📦 Video Segments: {total_c} Total &bull; {done_c} Done &bull; {active_c} Active</span>
                        <span style="color: #a6e3a1;">Max 2 Chunks / GPU</span>
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 8px;">
                """)
                for c_idx, c_st in sorted(chunks.items(), key=lambda x: int(x[0])):
                    c_pct = float(c_st.get("pct", 0.0))
                    c_spd = html.escape(str(c_st.get("speed", "--")))
                    c_status = str(c_st.get("status", "Queued"))
                    c_worker = html.escape(str(c_st.get("worker_name", c_st.get("worker_id", "GPU"))))
                    if c_status == "Done":
                        c_color = "#a6e3a1"
                        st_label = "✅ Done"
                    elif c_status in ["Encoding", "Retrying (CPU)"]:
                        c_color = "#f9e2af"
                        st_label = f"⚡ {c_spd}"
                    else:
                        c_color = "#6c7086"
                        st_label = "⏳ Queued"

                    html_parts.append(f"""
                        <div style="background: #11111b; border: 1px solid #313244; border-radius: 6px; padding: 6px 8px;">
                            <div style="display: flex; justify-content: space-between; font-size: 10px; color: #cdd6f4; font-weight: 600; margin-bottom: 2px;">
                                <span>Chunk #{int(c_idx) + 1}</span>
                                <span style="color: {c_color};">{st_label}</span>
                            </div>
                            <div style="font-size: 9px; color: #7f849c; margin-bottom: 4px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                                {c_worker}
                            </div>
                            <div style="background: #313244; border-radius: 3px; height: 4px; width: 100%; overflow: hidden;">
                                <div style="background: {c_color}; height: 100%; width: {c_pct:.1f}%;"></div>
                            </div>
                        </div>
                    """)
                html_parts.append("</div></div>")

            html_parts.append("</div>")

    # 2. Upcoming Queued Refining Jobs Section
    html_parts.append('<div style="font-weight: 700; font-size: 15px; color: #bac2de; display: flex; align-items: center; gap: 8px; margin-top: 10px;">📋 Persistent Refining Queue (queue.db) (' + str(len(pending_jobs)) + ')</div>')
    if not pending_jobs:
        html_parts.append("""
        <div style="background: #181825; border: 1px dashed #313244; border-radius: 10px; padding: 14px; text-align: center; color: #6c7086; font-size: 13px;">
            Refining queue is currently empty.
        </div>
        """)
    else:
        for idx, job in enumerate(pending_jobs):
            j_id = job["id"]
            title = html.escape(job.get("title", "Media"))
            m_type = job.get("media_type", "other")
            badge_icon = "🎬" if m_type == "movie" else ("📺" if m_type == "tv_series" else "📁")
            cfg = job.get("refining_config", {})
            res_tag = cfg.get("res_preset", "480p").split()[0]

            html_parts.append(f"""
            <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 10px 14px; display: flex; justify-content: space-between; align-items: center;">
                <div style="display: flex; align-items: center; gap: 10px; max-width: 70%;">
                    <span style="font-size: 12px; font-weight: 700; color: #585b70; width: 24px;">#{idx + 1}</span>
                    <span style="font-size: 14px; color: #cdd6f4; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
                        {badge_icon} {title}
                    </span>
                    <span style="background: #313244; color: #a6adc8; font-size: 11px; padding: 2px 6px; border-radius: 4px;">{res_tag}</span>
                </div>
                <div style="display: flex; align-items: center; gap: 6px;">
                    <button onclick="window.moveQueueItem('refine_queue', {j_id}, 'up')" title="Move Up in Refining Queue" style="background: #313244; color: #cdd6f4; border: 1px solid #45475a; border-radius: 4px; padding: 3px 8px; font-size: 11px; cursor: pointer;">
                        ⬆️ Up
                    </button>
                    <button onclick="window.moveQueueItem('refine_queue', {j_id}, 'down')" title="Move Down in Refining Queue" style="background: #313244; color: #cdd6f4; border: 1px solid #45475a; border-radius: 4px; padding: 3px 8px; font-size: 11px; cursor: pointer;">
                        ⬇️ Down
                    </button>
                    <button onclick="window.cancelTask('refine_queue', {j_id})" title="Remove from refining queue" style="background: rgba(243, 139, 168, 0.1); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.3); border-radius: 4px; padding: 3px 8px; font-size: 11px; cursor: pointer;">
                        ✕
                    </button>
                </div>
            </div>
            """)

    # 3. History: Completed, Failed with reason, Cancelled with reason
    if history_jobs:
        html_parts.append('<div style="font-weight: 700; font-size: 14px; color: #a6adc8; display: flex; align-items: center; gap: 8px; margin-top: 14px;">📜 Refined & Processed History (' + str(len(history_jobs)) + ')</div>')
        for job in history_jobs:
            j_id = job["id"]
            title = html.escape(job.get("title", "Media"))
            st = job.get("status", "completed").lower()
            err_msg = html.escape((job.get("error_msg") or "").strip())
            m_type = job.get("media_type", "other")
            badge_icon = "🎬" if m_type == "movie" else ("📺" if m_type == "tv_series" else "📁")

            if st == "completed":
                f_size = job.get("final_size_mb", 0.0)
                dest = html.escape(os.path.basename(job.get("target_refined_path", "") or "Google Drive"))
                badge_html = '<span style="background: rgba(166, 227, 161, 0.15); color: #a6e3a1; border: 1px solid rgba(166, 227, 161, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">✨ REFINED</span>'
                callout_html = f'<div style="font-size: 12px; color: #a6adc8; margin-top: 4px;">Saved: <b>{dest}</b> ({f_size:.1f} MB) • Raw Drive file unlinked via trash bypass.</div>'
            elif st == "failed":
                badge_html = '<span style="background: rgba(243, 139, 168, 0.15); color: #f38ba8; border: 1px solid rgba(243, 139, 168, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">❌ FAILED</span>'
                callout_html = f'<div style="margin-top: 6px; padding: 6px 10px; background: rgba(243, 139, 168, 0.1); border-left: 3px solid #f38ba8; border-radius: 4px; font-size: 12px; color: #f38ba8;">❌ Error Reason: {err_msg or "Refining transcode failed."}</div>'
            else:
                badge_html = '<span style="background: rgba(249, 226, 175, 0.15); color: #f9e2af; border: 1px solid rgba(249, 226, 175, 0.4); font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">⚠️ CANCELLED</span>'
                callout_html = f'<div style="margin-top: 6px; padding: 6px 10px; background: rgba(249, 226, 175, 0.1); border-left: 3px solid #f9e2af; border-radius: 4px; font-size: 12px; color: #f9e2af;">⚠️ Reason: {err_msg or "Cancelled by user."}</div>'

            html_parts.append(f"""
            <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 10px 14px; margin-bottom: 4px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="font-size: 13px; color: #cdd6f4; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 75%;">
                        {badge_icon} #{j_id}: {title}
                    </div>
                    <div style="display: flex; align-items: center; gap: 8px;">
                        {badge_html}
                        <button onclick="window.dismissTask('refine_queue', {j_id})" title="Dismiss from list" style="background: transparent; color: #6c7086; border: 1px solid #313244; border-radius: 4px; padding: 2px 6px; font-size: 10px; cursor: pointer;">
                            ✕
                        </button>
                    </div>
                </div>
                {callout_html}
            </div>
            """)

    html_parts.append('</div>')
    return "\n".join(html_parts)

def render_catalog_html() -> str:
    """Renders real-time media catalog from drive/downloader/data/metadata.json in strict ascending numerical order."""
    data = metadata_manager.load_metadata()

    movies = data.get("movies", [])
    tv_shows = data.get("tvseris", [])
    other = data.get("other", [])

    def _extract_num(key_str: Any) -> int:
        m = re.search(r'\d+', str(key_str))
        return int(m.group(0)) if m else 999999

    html_parts = ['<div style="display: flex; flex-direction: column; gap: 20px; font-family: system-ui, -apple-system, sans-serif;">']

    # Section 1: Movies (Sorted alphabetically and by release year)
    html_parts.append(f'<div style="font-weight: 700; font-size: 16px; color: #f9e2af; display: flex; align-items: center; gap: 8px;">🎬 Movies ({len(movies)})</div>')
    if not movies:
        html_parts.append('<div style="background: #181825; border: 1px dashed #313244; border-radius: 8px; padding: 12px; color: #6c7086; font-size: 13px;">No movies recorded yet.</div>')
    else:
        sorted_movies = sorted(
            movies,
            key=lambda m: (str(m.get("name", "")).lower(), int(m.get("year") or 0))
        )
        for m in sorted_movies:
            name = html.escape(m.get("name", "Movie"))
            st = m.get("status", "raw")
            is_ref = (st == "refined")
            badge_bg = "#1e382b" if is_ref else "#3e3b24"
            badge_fg = "#a6e3a1" if is_ref else "#f9e2af"
            badge_label = "REFINED" if is_ref else "RAW STAGED"
            p_val = m.get("refined_path" if is_ref else "raw_path", "")
            p_disp = html.escape(os.path.basename(p_val) if p_val else "Path on Google Drive")

            html_parts.append(f"""
            <div style="background: #1e1e2e; border: 1px solid #313244; border-radius: 8px; padding: 12px 16px; display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <div style="font-weight: 600; font-size: 14px; color: #cdd6f4;">{name}</div>
                    <div style="font-size: 12px; color: #6c7086; margin-top: 2px;">📁 {p_disp}</div>
                </div>
                <span style="background: {badge_bg}; color: {badge_fg}; border: 1px solid {badge_fg}; font-weight: 700; font-size: 10px; padding: 2px 8px; border-radius: 10px;">
                    {badge_label}
                </span>
            </div>
            """)

    # Section 2: TV Series (Strictly ascending Season and Episode numbers)
    html_parts.append(f'<div style="font-weight: 700; font-size: 16px; color: #89b4fa; display: flex; align-items: center; gap: 8px; margin-top: 10px;">📺 TV Series ({len(tv_shows)})</div>')
    if not tv_shows:
        html_parts.append('<div style="background: #181825; border: 1px dashed #313244; border-radius: 8px; padding: 12px; color: #6c7086; font-size: 13px;">No TV series recorded yet.</div>')
    else:
        for s in sorted(tv_shows, key=lambda x: str(x.get("name", "")).lower()):
            show_name = html.escape(s.get("name", "Show"))
            seasons = s.get("seasons", {})
            total_eps = sum(len(eps) for eps in seasons.values())

            html_parts.append(f"""
            <div style="background: #1e1e2e; border: 1px solid #313244; border-radius: 8px; padding: 12px 16px; margin-bottom: 6px;">
                <div style="font-weight: 600; font-size: 15px; color: #cdd6f4; margin-bottom: 6px;">
                    📺 {show_name} <span style="font-size: 12px; color: #7f849c; font-weight: 400;">({total_eps} episode(s) tracked)</span>
                </div>
            """)
            # Sort seasons in ascending order: Season 1, Season 2...
            sorted_seasons = sorted(seasons.items(), key=lambda item: _extract_num(item[0]))
            for s_key, eps in sorted_seasons:
                s_label = s_key.replace("_", " ").title()
                html_parts.append(f'<div style="font-size: 12px; font-weight: 600; color: #89b4fa; margin: 6px 0 4px 8px;">• {s_label}:</div>')
                # Sort episodes in ascending order: Episode 1, Episode 2...
                sorted_eps = sorted(
                    eps.items(),
                    key=lambda item: item[1].get("episode", _extract_num(item[0])) if isinstance(item[1], dict) and item[1].get("episode") is not None else _extract_num(item[0])
                )
                for ep_key, ep_data in sorted_eps:
                    ep_name = html.escape(ep_data.get("name", ep_key) if isinstance(ep_data, dict) else str(ep_key))
                    st = ep_data.get("status", "raw") if isinstance(ep_data, dict) else "raw"
                    is_ref = (st == "refined")
                    badge_bg = "#1e382b" if is_ref else "#3e3b24"
                    badge_fg = "#a6e3a1" if is_ref else "#f9e2af"
                    badge_label = "REFINED" if is_ref else "RAW"
                    html_parts.append(f"""
                    <div style="background: #181825; border: 1px solid #313244; border-radius: 6px; padding: 6px 12px; margin: 4px 0 4px 16px; display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-size: 13px; color: #a6adc8;">{ep_name}</span>
                        <span style="background: {badge_bg}; color: {badge_fg}; border: 1px solid {badge_fg}; font-weight: 700; font-size: 9px; padding: 1px 6px; border-radius: 8px;">
                            {badge_label}
                        </span>
                    </div>
                    """)
            html_parts.append("</div>")

    # Section 3: Other Media
    if other:
        html_parts.append(f'<div style="font-weight: 700; font-size: 16px; color: #fab387; display: flex; align-items: center; gap: 8px; margin-top: 10px;">📁 Other Media ({len(other)})</div>')
        for o in other:
            name = html.escape(o.get("name", "Media"))
            st = o.get("status", "raw")
            is_ref = (st == "refined")
            badge_bg = "#1e382b" if is_ref else "#3e3b24"
            badge_fg = "#a6e3a1" if is_ref else "#f9e2af"
            badge_label = "REFINED" if is_ref else "RAW"
            html_parts.append(f"""
            <div style="background: #1e1e2e; border: 1px solid #313244; border-radius: 8px; padding: 10px 14px; display: flex; justify-content: space-between; align-items: center;">
                <span style="font-size: 13px; color: #cdd6f4;">{name}</span>
                <span style="background: {badge_bg}; color: {badge_fg}; font-weight: 700; font-size: 9px; padding: 1px 6px; border-radius: 8px;">{badge_label}</span>
            </div>
            """)

    html_parts.append('</div>')
    return "\n".join(html_parts)

def render_cluster_html() -> str:
    """Renders GPU Cluster overview, active worker nodes, telemetry, and live chunk stats."""
    status = cluster_manager.get_cluster_status()
    workers = status.get("workers", [])
    total_nodes = status.get("total_nodes", 0)
    online_nodes = status.get("online_nodes", 0)
    encoding_nodes = status.get("encoding_nodes", 0)
    active_jobs = status.get("active_jobs", 0)
    total_chunks = status.get("total_chunks", 0)
    done_chunks = status.get("done_chunks", 0)

    html_parts = []
    html_parts.append('<div style="display: flex; flex-direction: column; gap: 16px;">')

    # 1. Top Metrics Banner
    html_parts.append(f"""
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px;">
        <div style="background: #1e1e2e; border: 1px solid #313244; border-radius: 10px; padding: 14px; text-align: center;">
            <div style="font-size: 11px; text-transform: uppercase; color: #a6adc8; font-weight: 700; letter-spacing: 0.5px;">Cluster Nodes</div>
            <div style="font-size: 24px; font-weight: 800; color: #a6e3a1; margin-top: 4px;">{online_nodes} <span style="font-size: 14px; color: #6c7086; font-weight: 500;">/ {total_nodes} Total</span></div>
        </div>
        <div style="background: #1e1e2e; border: 1px solid #313244; border-radius: 10px; padding: 14px; text-align: center;">
            <div style="font-size: 11px; text-transform: uppercase; color: #a6adc8; font-weight: 700; letter-spacing: 0.5px;">Active Transcodes</div>
            <div style="font-size: 24px; font-weight: 800; color: #89b4fa; margin-top: 4px;">{encoding_nodes} <span style="font-size: 14px; color: #6c7086; font-weight: 500;">Encoding</span></div>
        </div>
        <div style="background: #1e1e2e; border: 1px solid #313244; border-radius: 10px; padding: 14px; text-align: center;">
            <div style="font-size: 11px; text-transform: uppercase; color: #a6adc8; font-weight: 700; letter-spacing: 0.5px;">Cluster Chunks</div>
            <div style="font-size: 24px; font-weight: 800; color: #f9e2af; margin-top: 4px;">{done_chunks} <span style="font-size: 14px; color: #6c7086; font-weight: 500;">/ {total_chunks} Done</span></div>
        </div>
        <div style="background: #1e1e2e; border: 1px solid #313244; border-radius: 10px; padding: 14px; text-align: center;">
            <div style="font-size: 11px; text-transform: uppercase; color: #a6adc8; font-weight: 700; letter-spacing: 0.5px;">Active Refine Jobs</div>
            <div style="font-size: 24px; font-weight: 800; color: #cba6f7; margin-top: 4px;">{active_jobs} <span style="font-size: 14px; color: #6c7086; font-weight: 500;">In Pipeline</span></div>
        </div>
    </div>
    """)

    # 2. Worker Cards Section
    html_parts.append('<div style="font-weight: 700; font-size: 16px; color: #cdd6f4; display: flex; align-items: center; gap: 8px; margin-top: 8px;">🖥️ Registered GPU Compute Nodes</div>')

    if not workers:
        html_parts.append("""
        <div style="background: #181825; border: 1px dashed #313244; border-radius: 10px; padding: 24px; text-align: center; color: #a6adc8;">
            <div style="font-size: 18px; margin-bottom: 6px;">⏳ No Remote GPU Workers Connected Yet</div>
            <div style="font-size: 13px; color: #6c7086; max-width: 500px; margin: 0 auto;">
                Refining will use this notebook's local GPU. To connect external helper Colab notebooks, see the setup guide below.
            </div>
        </div>
        """)
    else:
        html_parts.append('<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 14px;">')
        now = time.time()
        for w in workers:
            w_id = html.escape(str(w.get("worker_id", "unknown")))
            node_name = html.escape(str(w.get("node_name", "GPU Worker")))
            gpu_name = html.escape(str(w.get("gpu_name", "GPU")))
            vram_mb = int(w.get("vram_mb", 0))
            vram_str = f"{vram_mb / 1024:.1f} GB" if vram_mb > 1024 else f"{vram_mb} MB"
            st = str(w.get("status", "idle")).lower()
            last_hb = float(w.get("last_heartbeat", 0.0))
            ago = max(0, int(now - last_hb)) if last_hb > 0 else 999
            chunks_done = int(w.get("total_chunks_processed", 0))
            job_id = int(w.get("current_job_id", 0))
            chunk_idx = int(w.get("current_chunk_idx", -1))
            pct = float(w.get("current_progress", 0.0))
            fps = float(w.get("current_fps", 0.0))
            speed = html.escape(str(w.get("current_speed", "--")))

            if st == "encoding":
                badge_bg = "rgba(249, 226, 175, 0.15)"
                badge_color = "#f9e2af"
                badge_text = "⚡ Encoding"
                border_color = "rgba(249, 226, 175, 0.4)"
            elif st == "offline" or ago > 45:
                badge_bg = "rgba(243, 139, 168, 0.15)"
                badge_color = "#f38ba8"
                badge_text = "🔴 Offline"
                border_color = "#313244"
            else:
                badge_bg = "rgba(166, 227, 161, 0.15)"
                badge_color = "#a6e3a1"
                badge_text = "🟢 Idle / Ready"
                border_color = "rgba(166, 227, 161, 0.3)"

            task_detail = ""
            if st == "encoding" and chunk_idx >= 0:
                task_detail = f"""
                <div style="margin-top: 10px; background: #11111b; border: 1px solid #313244; border-radius: 6px; padding: 8px 10px;">
                    <div style="display: flex; justify-content: space-between; font-size: 11px; color: #a6adc8; margin-bottom: 4px;">
                        <span>Job #{job_id} &bull; Chunk #{chunk_idx}</span>
                        <span style="font-weight: 700; color: #f9e2af;">{pct:.1f}% ({speed} / {fps:.0f} FPS)</span>
                    </div>
                    <div style="width: 100%; height: 6px; background: #313244; border-radius: 3px; overflow: hidden;">
                        <div style="width: {pct}%; height: 100%; background: #f9e2af; border-radius: 3px; transition: width 0.3s ease;"></div>
                    </div>
                </div>
                """

            html_parts.append(f"""
            <div style="background: #181825; border: 1px solid {border_color}; border-radius: 10px; padding: 14px; display: flex; flex-direction: column; justify-content: space-between;">
                <div>
                    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px;">
                        <div>
                            <div style="font-size: 15px; font-weight: 700; color: #cdd6f4;">{node_name}</div>
                            <div style="font-size: 11px; color: #6c7086; font-family: monospace;">{w_id}</div>
                        </div>
                        <span style="background: {badge_bg}; color: {badge_color}; border: 1px solid {badge_color}; border-radius: 12px; padding: 2px 8px; font-size: 11px; font-weight: 600;">
                            {badge_text}
                        </span>
                    </div>
                    <div style="display: flex; gap: 12px; font-size: 12px; color: #a6adc8; margin-top: 6px;">
                        <div>🎮 <strong>GPU:</strong> {gpu_name}</div>
                        <div>💾 <strong>VRAM:</strong> {vram_str}</div>
                    </div>
                    {task_detail}
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center; border-top: 1px solid #313244; padding-top: 10px; margin-top: 12px; font-size: 11px; color: #6c7086;">
                    <span>Completed Chunks: <strong style="color: #cdd6f4;">{chunks_done}</strong></span>
                    <span>Last seen: {ago}s ago</span>
                </div>
            </div>
            """)
        html_parts.append('</div>')

    # 3. Connection Instructions Accordion
    html_parts.append("""
    <div style="background: #1e1e2e; border: 1px solid #313244; border-radius: 10px; padding: 16px; margin-top: 10px;">
        <div style="font-weight: 700; font-size: 15px; color: #89b4fa; margin-bottom: 8px;">💡 How to Add GPU Helper Notebooks</div>
        <div style="font-size: 13px; color: #a6adc8; line-height: 1.6;">
            1. Open a second Google Colab tab or use another Google account.<br>
            2. Run <code>drive_downloader_gpu_helper.ipynb</code> located in your shared Google Drive <code>downloader</code> folder.<br>
            3. Mount Google Drive and select <strong>Runtime &gt; Change runtime type &gt; T4 GPU</strong> (or A100/V100).<br>
            4. Run the notebook. It will automatically detect this Main coordinator via <code>data/cluster/master.json</code> on Drive, join the cluster, and immediately start helping transcode video chunks!
        </div>
    </div>
    </div>
    """)

    return "".join(html_parts)

# ---------------------------------------------------------
# Build Main Gradio Application
# ---------------------------------------------------------

def create_ui():
    database.init_db()

    custom_css = """
    .gradio-container { background-color: #0f0f17 !important; color: #cdd6f4 !important; font-family: system-ui, -apple-system, sans-serif !important; }
    .panel-box { background: #161622 !important; border: 1px solid #2a2b3d !important; border-radius: 12px !important; padding: 16px !important; }
    .primary-btn { background: linear-gradient(135deg, #89b4fa, #b4befe) !important; color: #11111b !important; font-weight: 700 !important; border: none !important; border-radius: 8px !important; }
    .accent-btn { background: linear-gradient(135deg, #cba6f7, #f5c2e7) !important; color: #11111b !important; font-weight: 700 !important; border: none !important; border-radius: 8px !important; }
    .hidden-action-component { position: fixed !important; left: -9999px !important; top: -9999px !important; width: 1px !important; height: 1px !important; opacity: 0.001 !important; pointer-events: none !important; z-index: -9999 !important; }
    """

    client_js = """
    <script>
    window.queueAction = function(action, table, id, direction) {
        try {
            const payload = JSON.stringify({ action: action, table: table || '', id: id, direction: direction || 'up' });
            
            // 1. Dispatch via native Gradio component in DOM
            const inp = document.querySelector('#queue_action_input textarea') || 
                        document.querySelector('#queue_action_input input');
            const btn = document.querySelector('#queue_action_btn');
            if (inp && btn) {
                inp.value = payload;
                inp.dispatchEvent(new Event('input', { bubbles: true }));
                btn.click();
            }
        } catch(err) {
            console.error("[QueueAction] DOM dispatch error:", err);
        }

        // 2. Dual fallback to HTTP API endpoint
        try {
            if (action === 'move') {
                fetch('/api/queue/move/' + table + '/' + id + '/' + (direction || 'up'), { method: 'POST' }).catch(e => {});
            } else if (action === 'switch_speed') {
                fetch('/api/queue/switch_speed/' + id, { method: 'POST' }).catch(e => {});
            } else if (action === 'cancel') {
                const ep = table ? ('/api/task/' + id + '/cancel?table=' + table) : ('/api/task/' + id + '/cancel');
                fetch(ep, { method: 'POST' }).catch(e => {});
            } else if (action === 'dismiss') {
                const ep = table ? ('/api/task/' + id + '/dismiss?table=' + table) : ('/api/task/' + id + '/dismiss');
                fetch(ep, { method: 'POST' }).catch(e => {});
            }
        } catch(e) {}
    };

    window.switchQueueSpeed = function(table, id) {
        window.queueAction('switch_speed', table, id, '');
    };

    window.cancelTask = function(tableOrId, maybeId) {
        let table = '';
        let id = tableOrId;
        if (maybeId !== undefined) {
            table = tableOrId;
            id = maybeId;
        }
        window.queueAction('cancel', table, id, '');
    };

    window.dismissTask = function(tableOrId, maybeId) {
        let table = '';
        let id = tableOrId;
        if (maybeId !== undefined) {
            table = tableOrId;
            id = maybeId;
        }
        window.queueAction('dismiss', table, id, '');
    };

    window.moveQueueItem = function(table, id, direction) {
        window.queueAction('move', table, id, direction);
    };

    window.rawAction = function(action, filename) {
        try {
            const payload = JSON.stringify({ action: action, filename: filename });
            const inp = document.querySelector('#queue_action_input textarea') || 
                        document.querySelector('#queue_action_input input');
            const btn = document.querySelector('#queue_action_btn');
            if (inp && btn) {
                inp.value = payload;
                inp.dispatchEvent(new Event('input', { bubbles: true }));
                btn.click();
            }
        } catch(err) {
            console.error("[RawAction] DOM dispatch error:", err);
        }
        try {
            const ep = action === 'raw_enqueue' ? '/api/raw/enqueue/' : '/api/raw/delete/';
            fetch(ep + encodeURIComponent(filename), { method: 'POST' }).catch(e => {});
        } catch(e) {}
    };
    </script>
    """

    with gr.Blocks(title="Google Drive Video Downloader & GPU Refiner", theme=gr.themes.Default(), css=custom_css, head=client_js) as demo:
        # Native hidden elements for instant, iframe-immune reordering & cancellation
        queue_action_input = gr.Textbox(elem_id="queue_action_input", elem_classes=["hidden-action-component"], label="ActionInput")
        queue_action_btn = gr.Button(elem_id="queue_action_btn", elem_classes=["hidden-action-component"])
        # Title & System Header
        gr.Markdown(
            """
            # 🚀 Two-Stage Google Drive Downloader & GPU Refiner
            **Stage 1: High-Speed Ingestion & Staging** (Colab Workspace $\\rightarrow$ AI Categorization $\\rightarrow$ Google Drive `raw/`)
            **Stage 2: Persistent GPU Refining** (VRAM NVENC $\\rightarrow$ `refined/<tv_series|movies|other>` $\\rightarrow$ Drive Trash Bin Bypass)
            """
        )

        # -----------------------------------------------------
        # TOP UNIVERSAL SETTINGS BAR (Persistent Across All Tabs)
        # -----------------------------------------------------
        with gr.Accordion("🌐 Universal Settings Bar (Ingestion & Output Refining)", open=True):
            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("#### 📥 Ingestion & Download Rules")
                    download_conn_slider = gr.Slider(minimum=1, maximum=16, value=8, step=1, label="Aria2c Connections / Stream Chunks")
                    download_workers_slider = gr.Slider(minimum=1, maximum=5, value=3, step=1, label="Max Concurrent Downloads")
                    debrid_provider = gr.Dropdown(
                        choices=["None (Direct / P2P Swarm)", "Real-Debrid", "AllDebrid", "TorBox", "Debrid-Link"],
                        value="None (Direct / P2P Swarm)",
                        label="Debrid Acceleration"
                    )
                    debrid_api_key = gr.Textbox(label="Debrid API Key (Optional)", type="password", placeholder="Enter key if using Debrid")

                with gr.Column(scale=1):
                    gr.Markdown("#### ⚙️ Refining & Transcoding Rules")
                    with gr.Row():
                        res_preset = gr.Dropdown(
                            choices=[
                                "480p (Fast / High Efficiency)",
                                "240p (Ultra Lightweight)",
                                "360p (Mobile Data Saver)",
                                "720p (HD Quality)",
                                "1080p (FHD Crisp)",
                                "Original (Keep Source Res)"
                            ],
                            value="480p (Fast / High Efficiency)",
                            label="Target Resolution"
                        )
                        v_codec = gr.Dropdown(
                            choices=["H.265 / HEVC (NVIDIA GPU NVENC / CPU)", "H.264 / AVC (Broad Compatibility)"],
                            value="H.265 / HEVC (NVIDIA GPU NVENC / CPU)",
                            label="Video Codec"
                        )
                        container_format = gr.Dropdown(choices=["mkv", "mp4"], value="mkv", label="Container")

                    with gr.Row():
                        audio_preset = gr.Dropdown(
                            choices=[
                                "Opus 64 kbps (High Fidelity / Clean Sync)",
                                "Opus 48 kbps (Ultra Compact)",
                                "AAC 128 kbps (Universal Audio)",
                                "AAC 96 kbps (Mobile Audio)",
                                "Direct Copy (Passthrough)"
                            ],
                            value="Opus 64 kbps (High Fidelity / Clean Sync)",
                            label="Audio Encoding"
                        )
                        crf_slider = gr.Slider(minimum=18, maximum=36, value=32, step=1, label="Quality Factor (CRF: 18=Best, 32=Default)")

                    with gr.Row():
                        parallel_workers_slider = gr.Slider(minimum=1, maximum=8, value=4, step=1, label="GPU VRAM Slicing Chunks")
                        refine_workers_slider = gr.Slider(minimum=1, maximum=3, value=1, step=1, label="Max Concurrent Refiners")

        # -----------------------------------------------------
        # MAIN NAVIGATION TABS
        # -----------------------------------------------------
        with gr.Tabs() as main_tabs:

            # =================================================
            # TAB 1: 📥 FETCH & INGEST
            # =================================================
            with gr.Tab("📥 Fetch & Ingest", id="tab_fetch"):
                with gr.Tabs() as fetch_subtabs:

                    # Sub-Tab 1: Direct Download
                    with gr.Tab("🔗 Direct & Streaming Download", id="subtab_direct"):
                        gr.Markdown("Paste video URLs, m3u8 streams, Videm embeds, or direct HTTP links. AI automatically standardizes the title!")
                        direct_input_box = gr.Textbox(
                            label="Direct URLs (One per line or 'URL | Custom Title')",
                            lines=4,
                            placeholder="https://example.com/stream.m3u8 | My Video Title\nhttps://videm.xyz/embed/movie/tt1234567"
                        )
                        direct_name_box = gr.Textbox(
                            label="Batch / Single Custom Title Override (Optional)",
                            placeholder="Leave empty for AI & scene automatic naming"
                        )
                        btn_queue_direct = gr.Button("📥 Ingest to Download Queue", elem_classes=["primary-btn"])
                        direct_action_msg = gr.Markdown(visible=False)

                    # Sub-Tab 2: Torrent & Magnet Ingest (Single unified input)
                    with gr.Tab("🧲 Torrent & Magnet Download", id="subtab_torrent"):
                        gr.Markdown("Provide a magnet link or upload a `.torrent` file to inspect folders and select files to download.")
                        with gr.Row():
                            torrent_magnet_input = gr.Textbox(
                                label="Magnet Link",
                                placeholder="magnet:?xt=urn:btih:...",
                                lines=2,
                                scale=3
                            )
                            torrent_file_upload = gr.File(
                                label="Upload .torrent File",
                                file_types=[".torrent"],
                                scale=2
                            )

                        with gr.Row():
                            btn_inspect_torrent = gr.Button("🔍 Inspect Torrent Files & Folder Structure", elem_classes=["accent-btn"], scale=2)
                            btn_quick_ingest_torrent = gr.Button("⚡ Ingest All Videos (Auto-Detect)", elem_classes=["primary-btn"], scale=2)
                        torrent_action_msg = gr.Markdown(visible=False)
                        torrent_meta_state = gr.State({})

                        with gr.Group(visible=False) as torrent_tree_group:
                            torrent_summary_html = gr.HTML()
                            with gr.Row():
                                torrent_folder_filter = gr.Dropdown(label="📁 Filter by Folder", choices=["All Folders"], value="All Folders")
                                btn_select_all_vids = gr.Button("Select All Videos")
                                btn_deselect_all_vids = gr.Button("Deselect All")

                            torrent_videos_checkbox = gr.CheckboxGroup(label="🎬 Media Files to Ingest", choices=[])
                            btn_queue_torrent_selected = gr.Button("📥 Ingest Selected Videos to Download Queue", elem_classes=["primary-btn"])

                    # Sub-Tab 3: Title Search & Swarm Explorer
                    with gr.Tab("🔎 Title Search & Swarm Explorer", id="subtab_search"):
                        gr.Markdown("Search for any Movie or TV Show to instantly discover live torrent swarms and direct streams.")
                        with gr.Row():
                            search_input = gr.Textbox(label="Title / Show Name", placeholder="e.g. Avatar Fire and Ash, Mentalist, FBI", scale=4)
                            search_type_radio = gr.Radio(choices=["All", "Movie", "TV Series"], value="All", label="Type", scale=1)
                            btn_run_search = gr.Button("🔍 Search Swarms", elem_classes=["accent-btn"], scale=1)

                        search_results_state = gr.State([])
                        selected_media_cache = gr.State(None)
                        tv_seasons_cache = gr.State({})
                        live_torrents_cache = gr.State({})

                        search_results_dropdown = gr.Dropdown(label="Select Search Match", choices=[], interactive=True, visible=False)
                        search_action_msg = gr.Markdown(visible=False)

                        # Movie Actions Panel
                        with gr.Group(visible=False) as movie_actions_panel:
                            movie_info_display = gr.Markdown()
                            with gr.Row():
                                btn_queue_movie = gr.Button("📥 Queue Movie for Ingestion", elem_classes=["primary-btn"])
                                btn_insert_movie_direct = gr.Button("📋 Copy URL to Direct Tab")

                        # TV Show Actions Panel
                        with gr.Group(visible=False) as tv_actions_panel:
                            tv_info_display = gr.Markdown()
                            with gr.Row():
                                tv_season_select = gr.Dropdown(label="Select Season", choices=[])
                                btn_select_all_eps = gr.Button("Select All Episodes")
                                btn_deselect_all_eps = gr.Button("Deselect All")

                            tv_episodes_checkbox = gr.CheckboxGroup(label="Season Episodes", choices=[])
                            with gr.Row():
                                btn_queue_selected_eps = gr.Button("📥 Queue Selected Episodes for Ingestion", elem_classes=["primary-btn"])
                                btn_insert_eps_direct = gr.Button("📋 Copy Episodes to Direct Tab")

                        # Swarms Panel
                        with gr.Group(visible=False) as live_torrents_panel:
                            gr.Markdown("#### 🌊 Active High-Seed Torrents (APIBay & YTS.lu)")
                            live_torrents_dropdown = gr.Dropdown(label="Available Torrents (Ranked by Seeds)", choices=[])
                            with gr.Row():
                                btn_quick_queue_swarm = gr.Button("📥 Ingest Swarm to Download Queue", elem_classes=["primary-btn"])
                                btn_send_to_explorer = gr.Button("🗂️ Inspect Files in Torrent Tab")
                                btn_refresh_torrents = gr.Button("🔄 Refresh Torrents")

            # =================================================
            # TAB 2: 📋 QUEUE & PROCESS MANAGER
            # =================================================
            with gr.Tab("📋 Queue & Process Manager", id="tab_queue"):
                with gr.Tabs() as queue_subtabs:

                    # Sub-Tab 1: Download Queue
                    with gr.Tab("📥 Download Queue", id="subtab_dl_queue"):
                        with gr.Row():
                            btn_refresh_dl_queue = gr.Button("🔄 Refresh Download Queue")
                            btn_clear_dl_done = gr.Button("🧹 Clear Completed Downloads")

                        download_queue_html = gr.HTML(value=render_download_queue_html)

                    # Sub-Tab 2: Refining Queue
                    with gr.Tab("⚙️ Refining Queue (Persistent queue.db)", id="subtab_refine_queue"):
                        with gr.Row():
                            btn_refresh_refine_queue = gr.Button("🔄 Refresh Refining Queue")
                            btn_toggle_refine_pause = gr.Button("⏸️ Pause / Resume Refiner")
                            btn_clear_refine_done = gr.Button("🧹 Clear Completed Refined")

                        refine_status_msg = gr.Markdown("🟢 Refiner Daemon is active and monitoring persistent SQLite queue.")
                        refine_queue_html = gr.HTML(value=render_refine_queue_html)

            # =================================================
            # TAB 3: 📚 DRIVE MEDIA CATALOG
            # =================================================
            with gr.Tab("📚 Drive Media Catalog", id="tab_catalog"):
                with gr.Row():
                    gr.Markdown("### 🗄️ Google Drive Catalog (`drive/downloader/data/metadata.json`)")
                    btn_refresh_catalog = gr.Button("🔄 Refresh Catalog", scale=1)

                catalog_html_component = gr.HTML(value=render_catalog_html)

            # =================================================
            # TAB 4: 🖥️ GPU CLUSTER & HELPERS
            # =================================================
            with gr.Tab("🖥️ GPU Cluster & Helpers", id="tab_cluster"):
                with gr.Row():
                    gr.Markdown("### 🖥️ Distributed GPU Refiner Cluster & Active Helpers")
                    btn_refresh_cluster = gr.Button("🔄 Refresh Nodes", scale=1)
                    btn_cleanup_offline = gr.Button("🧹 Clear Offline Nodes", scale=1)

                cluster_html_component = gr.HTML(value=render_cluster_html)

        # Periodic Refresh Timer for Live Queue Updating
        refresh_timer = gr.Timer(value=3.0)
        refresh_timer.tick(
            fn=lambda: (render_download_queue_html(), render_refine_queue_html(), render_cluster_html()),
            outputs=[download_queue_html, refine_queue_html, cluster_html_component]
        )

        # -----------------------------------------------------
        # EVENT HANDLERS
        # -----------------------------------------------------

        # Worker Sliders Updates
        def update_download_workers(val):
            config.MAX_DOWNLOAD_WORKERS = int(val)
            config.download_semaphore = threading.Semaphore(int(val))

        download_workers_slider.change(fn=update_download_workers, inputs=[download_workers_slider])

        def update_refine_workers(val):
            config.MAX_REFINE_WORKERS = int(val)
            config.conversion_semaphore = threading.Semaphore(int(val))

        refine_workers_slider.change(fn=update_refine_workers, inputs=[refine_workers_slider])

        # Pause / Resume Refiner Daemon
        def handle_toggle_refiner():
            config.refine_worker_active = not config.refine_worker_active
            if config.refine_worker_active:
                return "🟢 Refiner Daemon is active and processing queue.", render_refine_queue_html()
            else:
                return "⏸️ Refiner Daemon is PAUSED. Jobs remain pending in queue.db.", render_refine_queue_html()

        btn_toggle_refine_pause.click(
            fn=handle_toggle_refiner,
            outputs=[refine_status_msg, refine_queue_html]
        )

        # Clear Completed Handlers
        btn_clear_dl_done.click(
            fn=lambda: (clear_done_tasks(), render_download_queue_html())[1],
            outputs=[download_queue_html]
        )

        btn_clear_refine_done.click(
            fn=lambda: (database.clear_completed("refine_queue"), render_refine_queue_html())[1],
            outputs=[refine_queue_html]
        )

        # Manual Refresh Buttons
        btn_refresh_dl_queue.click(fn=render_download_queue_html, outputs=[download_queue_html])
        btn_refresh_refine_queue.click(fn=render_refine_queue_html, outputs=[refine_queue_html])
        btn_refresh_catalog.click(fn=render_catalog_html, outputs=[catalog_html_component])
        btn_refresh_cluster.click(fn=render_cluster_html, outputs=[cluster_html_component])
        btn_cleanup_offline.click(
            fn=lambda: (database.cleanup_stale_cluster_workers(timeout_sec=0.0), render_cluster_html())[1],
            outputs=[cluster_html_component]
        )

        # Native Queue Reordering & Cancellation Handler (Dual Gradio DOM + API mechanism)
        def handle_queue_action(action_str: str):
            if not action_str:
                return render_download_queue_html(), render_refine_queue_html()
            try:
                data = json.loads(action_str)
                action = data.get("action")
                table = data.get("table", "")
                item_id = int(data.get("id", 0))
                direction = data.get("direction", "up")

                if action == "move" and item_id > 0 and table:
                    database.move_queue_item(table, item_id, direction)
                elif action == "switch_speed" and item_id > 0:
                    database.toggle_download_queue_speed(item_id)
                elif action == "dismiss" and item_id > 0 and table:
                    cancel_task_by_id(item_id, table_name=table, dismiss=True)
                elif action == "cancel" and item_id > 0:
                    cancel_task_by_id(item_id, table_name=table if table else None, dismiss=False)
                elif action == "raw_enqueue":
                    fn = data.get("filename", "")
                    if fn:
                        enqueue_unqueued_raw_file(fn)
                elif action == "raw_delete":
                    fn = data.get("filename", "")
                    if fn:
                        delete_unqueued_raw_file(fn)
            except Exception as e:
                print(f"[QueueAction] Handler error: {e}")
            return render_download_queue_html(), render_refine_queue_html()

        queue_action_btn.click(
            fn=handle_queue_action,
            inputs=[queue_action_input],
            outputs=[download_queue_html, refine_queue_html]
        )

        # 1. Queue Direct Items
        def handle_queue_direct(urls_text, title_override, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not urls_text.strip():
                return gr.update(visible=True, value="⚠️ Please provide at least one URL."), gr.update(), gr.update()

            lines = [l.strip() for l in urls_text.strip().split("\n") if l.strip()]
            items = []
            for line in lines:
                if "|" in line:
                    u, t = line.split("|", 1)
                    items.append((u.strip(), t.strip()))
                else:
                    items.append((line, title_override.strip() if title_override.strip() else ""))

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

            msg = f"✅ Ingested {count} item(s) into Download Queue!"
            # Automatically switch to Queue Tab
            return gr.update(visible=True, value=msg), render_download_queue_html(), gr.Tabs(selected="tab_queue")

        btn_queue_direct.click(
            fn=handle_queue_direct,
            inputs=[
                direct_input_box, direct_name_box,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                parallel_workers_slider, crf_slider
            ],
            outputs=[direct_action_msg, download_queue_html, main_tabs]
        )

        # 2. Inspect Torrent File / Magnet
        def handle_inspect_torrent(magnet_str, file_obj):
            source = (magnet_str or "").strip()
            if file_obj:
                fp = extract_file_path(file_obj)
                if fp and os.path.exists(fp):
                    source = fp

            if not source:
                return (
                    gr.update(visible=False), "",
                    gr.update(choices=["All Folders"], value="All Folders"),
                    gr.update(choices=[], value=[]),
                    gr.update(visible=True, value="⚠️ Please paste a magnet link or upload a .torrent file."),
                    {}
                )

            try:
                meta = fetch_torrent_metadata(source)
                files = meta.get("files", [])
                vids = [f for f in files if f.get("is_video")]
                folders = ["All Folders"] + meta.get("folders", [])

                summary = f"""
                <div style="background: #181825; border: 1px solid #313244; border-radius: 8px; padding: 12px; margin-bottom: 12px;">
                    <div style="font-weight: 700; font-size: 15px; color: #89b4fa;">🧲 {html.escape(meta['name'])}</div>
                    <div style="font-size: 13px; color: #a6adc8; margin-top: 4px;">
                        Total Size: <b>{meta['total_size_str']}</b> | Video Files: <b>{meta['video_count']}</b> | Folders: <b>{len(folders)-1}</b>
                    </div>
                </div>
                """

                vid_choices = [f["label"] for f in vids]
                return (
                    gr.update(visible=True), summary,
                    gr.update(choices=folders, value="All Folders"),
                    gr.update(choices=vid_choices, value=vid_choices),
                    gr.update(visible=False),
                    meta
                )
            except Exception as e:
                return (
                    gr.update(visible=False), "",
                    gr.update(choices=["All Folders"], value="All Folders"),
                    gr.update(choices=[], value=[]),
                    gr.update(visible=True, value=f"❌ Error parsing torrent: {html.escape(str(e))}"),
                    {}
                )

        btn_inspect_torrent.click(
            fn=handle_inspect_torrent,
            inputs=[torrent_magnet_input, torrent_file_upload],
            outputs=[
                torrent_tree_group, torrent_summary_html,
                torrent_folder_filter, torrent_videos_checkbox,
                torrent_action_msg, torrent_meta_state
            ]
        )

        # Filter by folder in Torrent
        def handle_folder_filter(folder_val, meta):
            if not meta or "files" not in meta:
                return gr.update(choices=[], value=[])
            vids = [f for f in meta["files"] if f.get("is_video")]
            if folder_val != "All Folders":
                vids = [f for f in vids if f.get("folder") == folder_val]
            labels = [f["label"] for f in vids]
            return gr.update(choices=labels, value=labels)

        torrent_folder_filter.change(
            fn=handle_folder_filter,
            inputs=[torrent_folder_filter, torrent_meta_state],
            outputs=[torrent_videos_checkbox]
        )

        btn_select_all_vids.click(
            fn=lambda folder_val, meta: handle_folder_filter(folder_val, meta),
            inputs=[torrent_folder_filter, torrent_meta_state],
            outputs=[torrent_videos_checkbox]
        )

        btn_deselect_all_vids.click(
            fn=lambda: gr.update(value=[]),
            outputs=[torrent_videos_checkbox]
        )

        # Queue Selected Videos from Torrent
        def handle_queue_torrent_videos(selected_labels, meta, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not selected_labels or not meta:
                return "⚠️ Please select at least one video to ingest.", render_download_queue_html(), gr.update()

            label_to_file = {f["label"]: f for f in meta.get("files", []) if f.get("is_video")}
            source_url = meta.get("cached_file_path") or meta.get("source_url")
            torrent_name = meta.get("name", "Torrent")

            count = 0
            for label in selected_labels:
                f = label_to_file.get(label)
                if not f:
                    continue

                refining_cfg = {
                    "res_preset": str(res_p),
                    "video_codec": str(v_cod),
                    "container_format": str(cont).lower(),
                    "audio_preset": str(aud_p),
                    "crf_val": int(crf_v),
                    "parallel_workers": int(workers),
                    "download_connections": int(d_conn),
                    "torrent_file_index": f.get("index", 1),
                    "target_file_index": f.get("index", 1),
                    "torrent_filename": f.get("filename"),
                    "rel_folder": f.get("folder", "Root"),
                    "torrent_cached_path": meta.get("cached_file_path"),
                    "has_custom_title": True
                }

                database.enqueue_download({
                    "title": sanitize_filename(f["filename_clean"]),
                    "url": source_url,
                    "source_type": "torrent",
                    "refining_config": refining_cfg
                })
                count += 1

            from pipeline import ensure_daemons_running
            ensure_daemons_running()

            msg = f"✅ Ingested {count} video(s) from '{torrent_name}' into Download Queue!"
            return msg, render_download_queue_html(), gr.Tabs(selected="tab_queue")

        btn_queue_torrent_selected.click(
            fn=handle_queue_torrent_videos,
            inputs=[
                torrent_videos_checkbox, torrent_meta_state,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                parallel_workers_slider, crf_slider
            ],
            outputs=[torrent_action_msg, download_queue_html, main_tabs]
        )

        # Quick Ingest: Automatically inspects and adds all detected video files
        def handle_quick_ingest_torrent(magnet_str, file_obj, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            source = (magnet_str or "").strip()
            if file_obj:
                fp = extract_file_path(file_obj)
                if fp and os.path.exists(fp):
                    source = fp

            if not source:
                return gr.update(visible=True, value="⚠️ Please paste a magnet link or upload a .torrent file."), render_download_queue_html(), gr.update()

            try:
                meta = fetch_torrent_metadata(source)
                vids = [f for f in meta.get("files", []) if f.get("is_video")]
                if not vids:
                    return gr.update(visible=True, value="⚠️ No video files detected in this torrent."), render_download_queue_html(), gr.update()

                source_url = meta.get("cached_file_path") or meta.get("source_url") or source
                torrent_name = meta.get("name", "Torrent")

                count = 0
                for f in vids:
                    refining_cfg = {
                        "res_preset": str(res_p),
                        "video_codec": str(v_cod),
                        "container_format": str(cont).lower(),
                        "audio_preset": str(aud_p),
                        "crf_val": int(crf_v),
                        "parallel_workers": int(workers),
                        "download_connections": int(d_conn),
                        "torrent_file_index": f.get("index", 1),
                        "target_file_index": f.get("index", 1),
                        "torrent_filename": f.get("filename"),
                        "rel_folder": f.get("folder", "Root"),
                        "torrent_cached_path": meta.get("cached_file_path"),
                        "has_custom_title": True
                    }

                    database.enqueue_download({
                        "title": sanitize_filename(f["filename_clean"]),
                        "url": source_url,
                        "source_type": "torrent",
                        "refining_config": refining_cfg
                    })
                    count += 1

                from pipeline import ensure_daemons_running
                ensure_daemons_running()

                msg = f"✅ Auto-detected and ingested {count} video(s) from '{torrent_name}' into Download Queue!"
                return gr.update(visible=True, value=msg), render_download_queue_html(), gr.Tabs(selected="tab_queue")
            except Exception as e:
                return gr.update(visible=True, value=f"❌ Error: {e}"), render_download_queue_html(), gr.update()

        btn_quick_ingest_torrent.click(
            fn=handle_quick_ingest_torrent,
            inputs=[
                torrent_magnet_input, torrent_file_upload,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                parallel_workers_slider, crf_slider
            ],
            outputs=[torrent_action_msg, download_queue_html, main_tabs]
        )

        # 3. Title Search & Swarm Search
        def handle_run_search(query, search_type):
            if not query or not query.strip():
                return gr.update(visible=False), [], "⚠️ Please enter a title to search.", gr.update(visible=False), gr.update(visible=False), gr.update(visible=False)

            m_filter = "🎬 Movies" if search_type == "Movie" else ("📺 TV Series" if search_type == "TV Series" else "All")
            choices, cache, msg = search_media_imdb(query.strip(), media_type=m_filter)

            if not choices:
                return gr.update(visible=False), [], f"❌ No results found for '{query}'.", gr.update(visible=False), gr.update(visible=False), gr.update(visible=False)

            results = [cache[label] for label in choices]
            return (
                gr.update(visible=True, choices=choices, value=choices[0]),
                results,
                f"✅ Found {len(results)} matches for '{query}'.",
                gr.update(visible=False), gr.update(visible=False), gr.update(visible=False)
            )

        btn_run_search.click(
            fn=handle_run_search,
            inputs=[search_input, search_type_radio],
            outputs=[
                search_results_dropdown, search_results_state, search_action_msg,
                movie_actions_panel, tv_actions_panel, live_torrents_panel
            ]
        )

        # Match Selection from Search
        def handle_select_match(choice, results):
            if not choice or not results:
                return (
                    gr.update(visible=False), gr.update(visible=False), gr.update(visible=False),
                    None, {}, {}, gr.update(choices=[]), "", "", gr.update(choices=[])
                )

            media = next((r for r in results if r.get("display") == choice), None)
            if not media:
                try:
                    idx = [r.get("display") for r in results].index(choice)
                    media = results[idx]
                except Exception:
                    media = results[0]

            m_type = media.get("type", "movie")
            title = media.get("title", "")
            year = media.get("year", "")

            # Search live torrent swarms
            torrents_choices, torrent_map = search_live_torrents(title, year=year, mode=m_type)

            if m_type == "movie":
                info_text = f"### 🎬 {title} ({year})\n*Direct stream and live swarms loaded below.*"
                return (
                    gr.update(visible=True), gr.update(visible=False), gr.update(visible=True),
                    media, {}, torrent_map,
                    gr.update(choices=torrents_choices, value=torrents_choices[0] if torrents_choices else None),
                    info_text, "", gr.update(choices=[])
                )
            else:
                show_title, seasons_data = get_tv_seasons_data(media.get("id"))
                s_choices = [f"Season {s}" for s in sorted(seasons_data.keys())]
                first_s = min(seasons_data.keys()) if seasons_data else 1
                first_eps = seasons_data.get(first_s, [])
                ep_choices = [e["label"] for e in first_eps]
                info_text = f"### 📺 {show_title or title} ({year})\n*Loaded {len(seasons_data)} season(s) and live torrent swarms.*"
                return (
                    gr.update(visible=False), gr.update(visible=True), gr.update(visible=True),
                    media, seasons_data, torrent_map,
                    gr.update(choices=torrents_choices, value=torrents_choices[0] if torrents_choices else None),
                    "", info_text,
                    gr.update(choices=s_choices, value=s_choices[0] if s_choices else None)
                )

        search_results_dropdown.change(
            fn=handle_select_match,
            inputs=[search_results_dropdown, search_results_state],
            outputs=[
                movie_actions_panel, tv_actions_panel, live_torrents_panel,
                selected_media_cache, tv_seasons_cache, live_torrents_cache,
                live_torrents_dropdown, movie_info_display, tv_info_display, tv_season_select
            ]
        )

        # TV Season Change
        def handle_season_change(season_str, seasons_data, media):
            if not season_str or not seasons_data:
                return gr.update(choices=[], value=[]), gr.update(choices=[]), {}
            m = re.search(r'\d+', season_str)
            s_num = int(m.group(0)) if m else 1
            eps = seasons_data.get(s_num, [])
            labels = [e["label"] for e in eps]
            t_title = media.get("title", "") if media else ""
            t_year = media.get("year", "") if media else ""
            t_choices, t_map = search_live_torrents(t_title, year=t_year, mode="tv", season=s_num)
            return (
                gr.update(choices=labels, value=labels),
                gr.update(choices=t_choices, value=t_choices[0] if t_choices else None),
                t_map
            )

        tv_season_select.change(
            fn=handle_season_change,
            inputs=[tv_season_select, tv_seasons_cache, selected_media_cache],
            outputs=[tv_episodes_checkbox, live_torrents_dropdown, live_torrents_cache]
        )

        btn_select_all_eps.click(
            fn=lambda season_str, seasons_data: gr.update(value=[e["label"] for e in seasons_data.get(int(re.search(r'\d+', season_str).group(0)), [])]) if season_str and seasons_data else gr.update(value=[]),
            inputs=[tv_season_select, tv_seasons_cache],
            outputs=[tv_episodes_checkbox]
        )

        btn_deselect_all_eps.click(
            fn=lambda: gr.update(value=[]),
            outputs=[tv_episodes_checkbox]
        )

        # Queue Movie from Search
        def handle_queue_movie(info, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not info:
                return "⚠️ No movie selected.", render_download_queue_html(), gr.update()
            imdb_id = info.get("id")
            title = info.get("title", "Movie")
            year = info.get("year")
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
            return f"✅ Ingested '{full_title}' into Download Queue!", render_download_queue_html(), gr.Tabs(selected="tab_queue")

        btn_queue_movie.click(
            fn=handle_queue_movie,
            inputs=[
                selected_media_cache,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                parallel_workers_slider, crf_slider
            ],
            outputs=[search_action_msg, download_queue_html, main_tabs]
        )

        # Queue Selected TV Episodes from Search
        def handle_queue_episodes(selected_eps, season_str, seasons_data, info, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not selected_eps or not season_str or not seasons_data or not info:
                return "⚠️ Please select at least one episode to queue.", render_download_queue_html(), gr.update()

            s_num = int(re.search(r'\d+', season_str).group(0))
            show_name = info.get("title", "TV Show")
            ep_map = {e["label"]: e for e in seasons_data.get(s_num, [])}

            items = []
            for label in selected_eps:
                e_data = ep_map.get(label)
                if e_data:
                    ep_num = e_data["episode"]
                    ep_title = f"{show_name} S{s_num:02d}E{ep_num:02d} - {e_data['name']}"
                    items.append((e_data["embed_url"], ep_title))

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
            return f"✅ Ingested {count} episode(s) of '{show_name}' Season {s_num} into Download Queue!", render_download_queue_html(), gr.Tabs(selected="tab_queue")

        btn_queue_selected_eps.click(
            fn=handle_queue_episodes,
            inputs=[
                tv_episodes_checkbox, tv_season_select, tv_seasons_cache, selected_media_cache,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                parallel_workers_slider, crf_slider
            ],
            outputs=[search_action_msg, download_queue_html, main_tabs]
        )

        # Ingest Torrent Swarm directly
        def handle_queue_live_torrent(selected_label, t_map, d_conn, debrid_p, debrid_k, res_p, v_cod, cont, aud_p, workers, crf_v):
            if not selected_label or not t_map or selected_label not in t_map:
                return "⚠️ Please select a torrent from the list to queue.", render_download_queue_html(), gr.update()

            t_info = t_map[selected_label]
            mag = t_info["magnet"]
            title = sanitize_filename(t_info["title"])

            count = queue_direct_items(
                [(mag, title)],
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
            msg = f"✅ Ingested swarm '{title}' ({t_info.get('size_str')}) into Download Queue!"
            return msg, render_download_queue_html(), gr.Tabs(selected="tab_queue")

        btn_quick_queue_swarm.click(
            fn=handle_queue_live_torrent,
            inputs=[
                live_torrents_dropdown, live_torrents_cache,
                download_conn_slider, debrid_provider, debrid_api_key,
                res_preset, v_codec, container_format, audio_preset,
                parallel_workers_slider, crf_slider
            ],
            outputs=[search_action_msg, download_queue_html, main_tabs]
        )

        # Inspect Swarm in Torrent Explorer
        def handle_send_to_explorer(selected_label, t_map):
            if not selected_label or not t_map or selected_label not in t_map:
                return (
                    gr.update(), gr.update(visible=False), "",
                    gr.update(choices=["All Folders"], value="All Folders"),
                    gr.update(choices=[], value=[]),
                    gr.update(visible=True, value="⚠️ Please select a torrent from the list first."),
                    {}, gr.update()
                )
            t_info = t_map[selected_label]
            mag = t_info["magnet"]
            res = handle_inspect_torrent(mag, None)
            return (
                (gr.update(value=mag),) + res + (gr.Tabs(selected="subtab_torrent"),)
            )

        btn_send_to_explorer.click(
            fn=handle_send_to_explorer,
            inputs=[live_torrents_dropdown, live_torrents_cache],
            outputs=[
                torrent_magnet_input, torrent_tree_group, torrent_summary_html,
                torrent_folder_filter, torrent_videos_checkbox,
                torrent_action_msg, torrent_meta_state, fetch_subtabs
            ]
        )

        btn_refresh_torrents.click(
            fn=lambda media, season: search_live_torrents(media.get("title", ""), year=media.get("year", ""), mode="tv" if media.get("type") == "tv" else "movie", season=int(re.search(r'\d+', season).group(0)) if season and re.search(r'\d+', season) else None) if media else ([], {}),
            inputs=[selected_media_cache, tv_season_select],
            outputs=[live_torrents_dropdown, live_torrents_cache]
        )

        # Copy to Direct box helpers
        btn_insert_movie_direct.click(
            fn=lambda info: (f"https://videm.xyz/embed/movie/{info['id']}", f"{info['title']} ({info.get('year', '')})".strip()) if info else ("", ""),
            inputs=[selected_media_cache],
            outputs=[direct_input_box, direct_name_box]
        )

        def handle_copy_eps(selected_eps, season_str, seasons_data, info):
            if not selected_eps or not season_str or not seasons_data or not info:
                return "", ""
            s_num = int(re.search(r'\d+', season_str).group(0)) if re.search(r'\d+', season_str) else 1
            show_name = info.get("title", "TV Show")
            ep_map = {e["label"]: e for e in seasons_data.get(s_num, [])}
            urls = []
            names = []
            for l in selected_eps:
                e = ep_map.get(l)
                if e:
                    urls.append(e["embed_url"])
                    names.append(f"{show_name} S{s_num:02d}E{e['episode']:02d} - {e['name']}")
            return "\n".join(urls), "\n".join(names)

        btn_insert_eps_direct.click(
            fn=handle_copy_eps,
            inputs=[tv_episodes_checkbox, tv_season_select, tv_seasons_cache, selected_media_cache],
            outputs=[direct_input_box, direct_name_box]
        )

    # Attach FastAPI endpoints for real-time cancel and reordering
    @demo.app.post("/api/task/{task_id}/cancel")
    @demo.app.get("/api/task/{task_id}/cancel")
    def api_cancel_task(task_id: int, table: Optional[str] = None):
        cancelled = cancel_task_by_id(task_id, table_name=table, dismiss=False)
        return {"status": "ok", "task_id": task_id, "cancelled": cancelled}

    @demo.app.post("/api/task/{task_id}/dismiss")
    @demo.app.get("/api/task/{task_id}/dismiss")
    def api_dismiss_task(task_id: int, table: Optional[str] = None):
        dismissed = cancel_task_by_id(task_id, table_name=table, dismiss=True)
        return {"status": "ok", "task_id": task_id, "dismissed": dismissed}

    @demo.app.post("/api/queue/move/{table_name}/{item_id}/{direction}")
    def api_move_queue(table_name: str, item_id: int, direction: str):
        success = database.move_queue_item(table_name, item_id, direction)
        return {"status": "ok", "success": success}

    @demo.app.post("/api/queue/switch_speed/{item_id}")
    @demo.app.get("/api/queue/switch_speed/{item_id}")
    def api_switch_queue_speed(item_id: int):
        success = database.toggle_download_queue_speed(item_id)
        return {"status": "ok", "success": success, "item_id": item_id}

    @demo.app.get("/api/queue/download")
    def api_download_queue():
        return {"items": database.get_download_queue()}

    @demo.app.get("/api/queue/refine")
    def api_refine_queue():
        return {"items": database.get_refine_queue()}

    @demo.app.post("/api/raw/enqueue/{filename:path}")
    def api_raw_enqueue(filename: str):
        job_id = enqueue_unqueued_raw_file(filename)
        return {"status": "ok", "filename": filename, "job_id": job_id}

    @demo.app.post("/api/raw/delete/{filename:path}")
    def api_raw_delete(filename: str):
        success = delete_unqueued_raw_file(filename)
        return {"status": "ok", "filename": filename, "success": success}

    return demo
