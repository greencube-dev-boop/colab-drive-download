import sys
import time
import argparse
from ui import create_ui
from config import tasks, tasks_lock
import database

def main():
    parser = argparse.ArgumentParser(description="Two-Stage Google Drive Downloader & GPU Refiner")
    parser.add_argument("--port", type=int, default=7860, help="Port to bind the server on")
    parser.add_argument("--share", action="store_true", default=True, help="Create a public gradio.live share link")
    parser.add_argument("--no-share", dest="share", action="store_false", help="Disable public share link")
    args = parser.parse_args()

    print("=" * 72)
    print("🚀 Initializing Two-Stage Google Drive Downloader & GPU Refiner Engine...")
    print("=" * 72)

    database.init_db()
    demo = create_ui()

    print(f"\n🌐 Launching Web Interface on port {args.port} (share={args.share})...")
    demo.launch(
        server_port=args.port,
        share=args.share,
        inline=True,
        prevent_thread_lock=True
    )

    print("\n" + "=" * 72)
    print("🟢 Gradio Web Server & Two-Stage Pipelines are LIVE!")
    print("🔒 CELL KEEP-ALIVE ACTIVE:")
    print("   This cell runs continuously in a live heartbeat loop to keep")
    print("   the Colab kernel busy and prevent idle disconnects.")
    print("   Press the STOP button in Colab or Ctrl+C if you want to stop.")
    print("=" * 72 + "\n")

    start_keepalive = time.time()
    try:
        while True:
            time.sleep(5)
            elapsed = int(time.time() - start_keepalive)
            m, s = divmod(elapsed, 60)
            h, m = divmod(m, 60)
            dl_items = database.get_download_queue()
            rf_items = database.get_refine_queue()

            dl_active = sum(1 for it in dl_items if it.get("status") == "downloading")
            dl_pending = sum(1 for it in dl_items if it.get("status") == "pending")
            rf_active = sum(1 for it in rf_items if it.get("status") == "refining")
            rf_pending = sum(1 for it in rf_items if it.get("status") == "pending")
            rf_done = sum(1 for it in rf_items if it.get("status") == "completed")

            print(f"\r⏱️ Keep-Alive | Uptime: {h:02d}h {m:02d}m {s:02d}s | DLs: {dl_active} active ({dl_pending} queued) | Refiner: {rf_active} active ({rf_pending} queued) | Refined: {rf_done}", end="", flush=True)
    except KeyboardInterrupt:
        print("\n🛑 Execution stopped by user.")

if __name__ == "__main__":
    main()
