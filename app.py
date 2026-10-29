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
import logger

def main(enable_debug_logs: bool = True):
    parser = argparse.ArgumentParser(description="Two-Stage Google Drive Downloader & GPU Refiner")
    parser.add_argument("--port", type=int, default=7860, help="Port to bind the server on")
    parser.add_argument("--share", action="store_true", default=True, help="Create a public gradio.live share link")
    parser.add_argument("--no-share", dest="share", action="store_false", help="Disable public share link")
    parser.add_argument("--debug", action="store_true", default=None, help="Enable verbose debug log notifications")
    parser.add_argument("--no-debug", dest="debug", action="store_false", help="Disable verbose debug logs")
    args, _ = parser.parse_known_args()

    if args.debug is not None:
        enable_debug_logs = args.debug
    logger.set_debug(enable_debug_logs)
    logger.set_log_file(getattr(config, "SYSTEM_LOG_FILE", None))

    print("=" * 72)
    print("🚀 Initializing Two-Stage Google Drive Downloader & GPU Refiner Engine...")
    print(f"📋 Debug Log Notifications: {'ENABLED (Verbose mode)' if logger.is_debug() else 'DISABLED (Quiet mode)'}")
    print("=" * 72)

    config.cleanup_drive_root_stray_files()
    database.init_db()
    ensure_daemons_running()
    demo = create_ui()

    # Mount Distributed GPU Cluster REST API endpoints on Gradio's underlying FastAPI app
    import cluster_manager
    from fastapi import Request
    from fastapi.responses import JSONResponse

    cluster_manager.start_cluster_daemon()

    def mount_cluster_api(fastapi_app):
        if fastapi_app is None:
            return

        @fastapi_app.post("/api/cluster/register")
        @fastapi_app.post("/api/cluster/register/")
        async def api_register_worker(request: Request):
            try:
                data = await request.json()
            except Exception:
                data = {}
            res = cluster_manager.register_worker(data)
            return JSONResponse(content=res)

        @fastapi_app.post("/api/cluster/heartbeat")
        @fastapi_app.post("/api/cluster/heartbeat/")
        async def api_heartbeat_worker(request: Request):
            try:
                data = await request.json()
            except Exception:
                data = {}
            wid = data.get("worker_id", "")
            res = cluster_manager.heartbeat_worker(wid, data)
            return JSONResponse(content=res)

        @fastapi_app.get("/api/cluster/claim_task")
        @fastapi_app.get("/api/cluster/claim_task/")
        async def api_claim_task(worker_id: str = "", node_name: str = ""):
            res = cluster_manager.claim_task(worker_id, node_name=node_name)
            return JSONResponse(content=res)

        @fastapi_app.post("/api/cluster/complete_task")
        @fastapi_app.post("/api/cluster/complete_task/")
        async def api_complete_task(request: Request):
            try:
                data = await request.json()
            except Exception:
                data = {}
            wid = data.get("worker_id", "")
            job_id = int(data.get("job_id", 0))
            chunk_idx = int(data.get("chunk_idx", -1))
            out_chunk_path = data.get("out_chunk_path", "")
            fsize = float(data.get("file_size_mb", 0.0))
            res = cluster_manager.complete_task(wid, job_id, chunk_idx, out_chunk_path, fsize)
            return JSONResponse(content=res)

        @fastapi_app.post("/api/cluster/release_task")
        @fastapi_app.post("/api/cluster/release_task/")
        async def api_release_task(request: Request):
            try:
                data = await request.json()
            except Exception:
                data = {}
            wid = data.get("worker_id", "")
            job_id = int(data.get("job_id", 0))
            chunk_idx = int(data.get("chunk_idx", -1))
            res = cluster_manager.release_task(wid, job_id, chunk_idx)
            return JSONResponse(content=res)

        @fastapi_app.get("/api/cluster/chunk/{job_id}/{chunk_idx}")
        async def api_get_chunk(job_id: int, chunk_idx: int):
            try:
                chunk_path = cluster_manager.get_chunk_raw_path(job_id, chunk_idx)
                if not chunk_path or not os.path.exists(chunk_path):
                    return JSONResponse(status_code=404, content={"error": f"Chunk #{chunk_idx} of Job #{job_id} not found on disk"})
                fsize = os.path.getsize(chunk_path)
                if fsize == 0:
                    return JSONResponse(status_code=404, content={"error": f"Chunk #{chunk_idx} of Job #{job_id} is 0 bytes"})

                from fastapi.responses import StreamingResponse

                def iterfile():
                    with open(chunk_path, "rb") as f:
                        while chunk := f.read(1024 * 1024):
                            yield chunk

                headers = {
                    "Content-Length": str(fsize),
                    "Content-Disposition": f'attachment; filename="{os.path.basename(chunk_path)}"',
                    "Accept-Ranges": "bytes"
                }
                return StreamingResponse(iterfile(), media_type="video/x-matroska", headers=headers)
            except Exception as e:
                import logger
                logger.error("CLUSTER", f"Error streaming chunk #{chunk_idx} of Job #{job_id}: {e}")
                return JSONResponse(status_code=500, content={"error": str(e)})

        @fastapi_app.get("/api/cluster/status")
        @fastapi_app.get("/api/cluster/status/")
        async def api_cluster_status():
            status = cluster_manager.get_cluster_status()
            return JSONResponse(content=status)

    mount_cluster_api(getattr(demo, "app", None))

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

    # Re-mount cluster REST API routes on the active server app created by demo.launch()
    active_app = getattr(demo, "server_app", None) or getattr(demo, "app", None)
    mount_cluster_api(active_app)
    print("🔌 Distributed GPU Cluster REST API mounted on active web server.")

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

    # 5. Publish GPU Cluster Master Beacon to shared Google Drive
    primary_url = cf_url or gradio_share or f"http://127.0.0.1:{args.port}"
    cluster_manager.publish_master_beacon(primary_url)
    print(f"📡 GPU Cluster Master Beacon published to shared Drive ({primary_url})")

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
