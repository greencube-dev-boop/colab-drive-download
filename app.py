import sys
import time
import argparse
from ui import create_ui
from config import tasks, tasks_lock

def main():
    parser = argparse.ArgumentParser(description="Colab Video Downloader & 480p MKV Transcoder")
    parser.add_argument("--port", type=int, default=7860, help="Port to bind the server on")
    parser.add_argument("--share", action="store_true", default=True, help="Create a public gradio.live share link")
    parser.add_argument("--no-share", dest="share", action="store_false", help="Disable public share link")
    args = parser.parse_args()

    print("=" * 68)
    print("🚀 Initializing Colab Video Downloader & 480p Transcoder Engine...")
    print("=" * 68)

    demo = create_ui()

    print(f"\n🌐 Launching Web Interface on port {args.port} (share={args.share})...")
    demo.launch(
        server_port=args.port,
        share=args.share,
        inline=True,
        prevent_thread_lock=True
    )

    print("\n" + "=" * 68)
    print("🟢 Gradio Web Server & Queue Pipeline are LIVE!")
    print("🔒 CELL KEEP-ALIVE ACTIVE:")
    print("   This cell will run continuously in a live heartbeat loop to keep")
    print("   the Colab kernel busy and prevent idle disconnects.")
    print("   Press the STOP button in Colab or Ctrl+C if you want to stop.")
    print("=" * 68 + "\n")

    start_keepalive = time.time()
    try:
        while True:
            time.sleep(5)
            elapsed = int(time.time() - start_keepalive)
            m, s = divmod(elapsed, 60)
            h, m = divmod(m, 60)
            with tasks_lock:
                n_dl = sum(1 for t in tasks if t.get("status") == "Downloading")
                n_conv = sum(1 for t in tasks if t.get("status") == "Converting")
                n_q_dl = sum(1 for t in tasks if t.get("status") == "Queued for Download")
                n_q_cv = sum(1 for t in tasks if t.get("status") == "Queued for Conversion")
                n_done = sum(1 for t in tasks if t.get("status") == "Done")
            print(f"\r⏱️ Keep-Alive | Uptime: {h:02d}h {m:02d}m {s:02d}s | Active DL: {n_dl}/3 (Queued: {n_q_dl}) | Converting: {n_conv}/1 (Queued: {n_q_cv}) | Completed: {n_done}", end="", flush=True)
    except KeyboardInterrupt:
        print("\n🛑 Execution stopped by user.")

if __name__ == "__main__":
    main()
