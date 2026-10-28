import os
import sys
import time
import re
import shutil
import platform
import subprocess
import threading
import atexit
from typing import Optional

_tunnel_proc: Optional[subprocess.Popen] = None
_tunnel_url: Optional[str] = None
_tunnel_lock = threading.Lock()

def _get_cloudflared_path() -> Optional[str]:
    """Finds or downloads the static cloudflared binary."""
    # 1. Check system PATH
    system_path = shutil.which("cloudflared")
    if system_path:
        return system_path

    # 2. Check standard locations
    candidates = [
        "/usr/local/bin/cloudflared",
        "/usr/bin/cloudflared",
        "/tmp/cloudflared",
        "./cloudflared",
        "./cloudflared.exe"
    ]
    for c in candidates:
        if os.path.exists(c) and (os.access(c, os.X_OK) or os.name == 'nt'):
            return c

    # 3. If on Linux (Colab), attempt automatic download
    if platform.system() == "Linux":
        arch = platform.machine().lower()
        if "x86_64" in arch or "amd64" in arch:
            url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64"
        elif "arm" in arch or "aarch64" in arch:
            url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64"
        else:
            return None

        target = "/tmp/cloudflared"
        try:
            print("[Tunnel] Downloading Cloudflare Tunnel binary (cloudflared)...")
            import urllib.request
            urllib.request.urlretrieve(url, target)
            os.chmod(target, 0o755)
            if os.path.exists(target):
                return target
        except Exception as e:
            print(f"[Tunnel] Notice: Could not auto-download cloudflared ({e})")
            return None

    return None

def start_cloudflare_tunnel(port: int = 7860, timeout_sec: float = 12.0) -> Optional[str]:
    """
    Spawns an ephemeral Cloudflare Tunnel pointing to localhost:port.
    Returns the public https://*.trycloudflare.com URL if established.
    """
    global _tunnel_proc, _tunnel_url
    with _tunnel_lock:
        if _tunnel_url:
            return _tunnel_url

        binary = _get_cloudflared_path()
        if not binary:
            return None

        try:
            cmd = [binary, "tunnel", "--url", f"http://127.0.0.1:{port}"]
            _tunnel_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            atexit.register(stop_cloudflare_tunnel)

            url_found = None
            url_event = threading.Event()

            def _reader():
                nonlocal url_found
                for line in iter(_tunnel_proc.stderr.readline, ''):
                    if not line:
                        break
                    m = re.search(r'https://[a-zA-Z0-9-]+\.trycloudflare\.com', line)
                    if m:
                        url_found = m.group(0)
                        url_event.set()
                        break

            t = threading.Thread(target=_reader, daemon=True)
            t.start()

            url_event.wait(timeout=timeout_sec)
            if url_found:
                _tunnel_url = url_found
                return _tunnel_url
            else:
                return None
        except Exception as e:
            print(f"[Tunnel] Notice: Cloudflare tunnel could not start: {e}")
            return None

def stop_cloudflare_tunnel():
    """Terminates active cloudflared process on shutdown."""
    global _tunnel_proc, _tunnel_url
    with _tunnel_lock:
        if _tunnel_proc:
            try:
                _tunnel_proc.terminate()
                _tunnel_proc.wait(timeout=2)
            except Exception:
                try:
                    _tunnel_proc.kill()
                except Exception:
                    pass
            _tunnel_proc = None
        _tunnel_url = None

def setup_colab_access(port: int = 7860):
    """Configures Google Colab native port forwarding if running in a Colab kernel."""
    try:
        from google.colab import output
        output.serve_kernel_port_as_window(
            port,
            anchor_text="🔗 [Google Colab Native] Open Web Dashboard in New Tab"
        )
        output.serve_kernel_port_as_iframe(port, height=850)
    except Exception:
        pass

def safe_print(text: str):
    """Safely prints text handling non-UTF-8 console environments."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"))

def display_access_banner(port: int, cloudflare_url: Optional[str] = None, gradio_share_url: Optional[str] = None):
    """Prints a clear ASCII and rich HTML banner with all available access endpoints."""
    safe_print("\n" + "=" * 72)
    safe_print("WEB UI DASHBOARD ACCESS ENDPOINTS")
    safe_print("=" * 72)

    if cloudflare_url:
        safe_print(f"[Cloudflare Tunnel] (Recommended): {cloudflare_url}")
    else:
        safe_print("[Cloudflare Tunnel]: Not active (using local/share)")

    if gradio_share_url:
        safe_print(f"[Gradio Public Share]:            {gradio_share_url}")
    else:
        safe_print("[Gradio Public Share]:            Unavailable (FRP server offline/refused)")

    safe_print(f"[Local / Colab Direct]:           http://127.0.0.1:{port}")
    safe_print("=" * 72)

    # Attempt rich HTML rendering in Colab/Jupyter notebook
    try:
        from IPython.display import display, HTML
        buttons_html = []
        if cloudflare_url:
            buttons_html.append(
                f'<a href="{cloudflare_url}" target="_blank" style="background: #a6e3a1; color: #11111b; font-weight: 700; padding: 10px 18px; border-radius: 8px; text-decoration: none; font-size: 14px; display: inline-flex; align-items: center; gap: 6px;">Open via Cloudflare (Recommended)</a>'
            )
        if gradio_share_url:
            buttons_html.append(
                f'<a href="{gradio_share_url}" target="_blank" style="background: #89b4fa; color: #11111b; font-weight: 700; padding: 10px 18px; border-radius: 8px; text-decoration: none; font-size: 14px; display: inline-flex; align-items: center; gap: 6px;">Open via Gradio Share</a>'
            )

        html_banner = f"""
        <div style="background: #181825; border: 2px solid #89b4fa; border-radius: 12px; padding: 18px 24px; font-family: system-ui, -apple-system, sans-serif; margin: 15px 0; max-width: 820px;">
            <div style="font-size: 18px; font-weight: 800; color: #cdd6f4; margin-bottom: 6px;">
                Downloader & GPU Refiner Web Dashboard is LIVE!
            </div>
            <div style="font-size: 13px; color: #a6adc8; margin-bottom: 14px;">
                Click any link below to open the dashboard. Cloudflare tunnel is immune to Gradio FRP outages.
            </div>
            <div style="display: flex; gap: 12px; flex-wrap: wrap;">
                {' '.join(buttons_html)}
            </div>
        </div>
        """
        display(HTML(html_banner))
    except Exception:
        pass
