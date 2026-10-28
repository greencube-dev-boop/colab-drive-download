import sys
import time
import argparse
import logging
import warnings

# Suppress benign Gradio 6.0 deprecation/user warnings for Blocks constructor parameters
warnings.filterwarnings(
    "ignore",
    message=r".*parameter in the Blocks constructor will be removed in Gradio 6\.0.*"
)
warnings.filterwarnings(
    "ignore",
    message=r".*The parameters have been moved from the Blocks constructor to the launch\(\) method.*"
)

# Suppress harmless Starlette/Uvicorn ASGI disconnect tracebacks when browser tabs refresh or background
class ASGISilentDisconnectFilter(logging.Filter):
    """
    Suppresses benign ASGI stream cancellation tracebacks that occur when a browser
    client tab is closed, refreshed, or backgrounded while an SSE stream is in progress.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        msg = str(record.getMessage())
        if "response already started" in msg.lower():
            return False
        if record.exc_info:
            exc = record.exc_info[1]
            if exc and "response already started" in str(exc).lower():
                return False
        return True

for logger_name in ("uvicorn.error", "uvicorn", "uvicorn.access"):
    logging.getLogger(logger_name).addFilter(ASGISilentDisconnectFilter())

from ui import create_ui
import config
from config import tasks, tasks_lock
import database
from pipeline import ensure_daemons_running

def main():
    parser = argparse.ArgumentParser(description="Two-Stage Google Drive Downloader & GPU Refiner")
    parser.add_argument("--port", type=int, default=7860, help="Port to bind the server on")
    parser.add_argument("--share", action="store_true", default=True, help="Create a public gradio.live share link")
    parser.add_argument("--no-share", dest="share", action="store_false", help="Disable public share link")
    args, _ = parser.parse_known_args()

    print("=" * 72)
    print("🚀 Initializing Two-Stage Google Drive Downloader & GPU Refiner Engine...")
    print("=" * 72)

    config.cleanup_drive_root_stray_files()
    database.init_db()
    ensure_daemons_running()
    demo = create_ui()

    print(f"\n🌐 Launching Web Interface on port {args.port} (share={args.share})...")

    # 1. Start Cloudflare Tunnel in background (robust, immune to Gradio FRP outages)
    cf_url = None
    try:
        import tunnel
        cf_url = tunnel.start_cloudflare_tunnel(port=args.port)
    except Exception as e:
        print(f"[Tunnel] Notice starting Cloudflare tunnel: {e}")

    # 2. Launch Gradio server
    try:
        demo.launch(
            server_port=args.port,
            share=args.share,
            inline=True,
            prevent_thread_lock=True
        )
    except Exception as e:
        print(f"[App] Notice during demo.launch: {e}")
        if args.share:
            print("[App] Retrying launch in local mode (Cloudflare tunnel remains active)...")
            demo.launch(
                server_port=args.port,
                share=False,
                inline=True,
                prevent_thread_lock=True
            )

    # 3. Setup Colab native proxy links (if in Google Colab kernel)
    try:
        import tunnel
        tunnel.setup_colab_access(args.port)
    except Exception:
        pass

    # 4. Display Access Banner
    gradio_share = getattr(demo, "share_url", None)
    import tunnel
    tunnel.display_access_banner(args.port, cloudflare_url=cf_url, gradio_share_url=gradio_share)

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
        print("\n🛑 Execution stopped by user. Flushing persistent queues...")
    finally:
        database.sync_db_to_drive(force=True)

if __name__ == "__main__":
    main()
