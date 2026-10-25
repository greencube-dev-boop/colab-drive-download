import os
import re
import urllib.parse
import urllib.request
import subprocess
import shutil
from typing import Dict, Any, List, Optional, Tuple

MEDIA_EXTS = ('.mkv', '.mp4', '.avi', '.mov', '.ts', '.webm', '.flv', '.m4v', '.wmv', '.m2ts')

def format_torrent_size(size_bytes: int) -> str:
    sz = float(size_bytes)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if sz < 1024.0:
            return f"{sz:.1f} {unit}"
        sz /= 1024.0
    return f"{sz:.1f} PB"

def bdecode(data: bytes) -> Tuple[Any, int]:
    """Pure-Python Bencode Decoder."""
    if not data:
        raise ValueError("Empty data to bdecode")
    b = data[0:1]
    if b == b'i':
        end = data.find(b'e', 1)
        if end == -1:
            raise ValueError("Unterminated bencode integer")
        return int(data[1:end]), end + 1
    elif b == b'l':
        items = []
        idx = 1
        while idx < len(data) and data[idx:idx+1] != b'e':
            val, consumed = bdecode(data[idx:])
            items.append(val)
            idx += consumed
        return items, idx + 1
    elif b == b'd':
        d = {}
        idx = 1
        while idx < len(data) and data[idx:idx+1] != b'e':
            key, k_consumed = bdecode(data[idx:])
            idx += k_consumed
            val, v_consumed = bdecode(data[idx:])
            idx += v_consumed
            if isinstance(key, bytes):
                key = key.decode('utf-8', errors='replace')
            d[key] = val
        return d, idx + 1
    elif b.isdigit():
        colon = data.find(b':')
        if colon == -1:
            raise ValueError("Invalid bencode string prefix")
        length = int(data[:colon])
        start = colon + 1
        end = start + length
        return data[start:end], end
    else:
        raise ValueError(f"Invalid bencode token: {b}")

def bdecode_torrent_name(data: bytes) -> Optional[str]:
    try:
        decoded, _ = bdecode(data)
        if isinstance(decoded, dict) and "info" in decoded:
            info = decoded["info"]
            name_bytes = info.get("name.utf-8") or info.get("name")
            if isinstance(name_bytes, bytes):
                return name_bytes.decode("utf-8", errors="replace")
            elif isinstance(name_bytes, str):
                return name_bytes
    except Exception:
        pass
    return None

def parse_torrent_bytes(raw_bytes: bytes) -> Dict[str, Any]:
    decoded, _ = bdecode(raw_bytes)
    if not isinstance(decoded, dict) or "info" not in decoded:
        raise ValueError("Invalid torrent file format: missing 'info' dictionary.")
    info = decoded["info"]

    name_bytes = info.get("name.utf-8") or info.get("name")
    torrent_name = (name_bytes.decode('utf-8', errors='replace') if isinstance(name_bytes, bytes) else str(name_bytes or "Torrent"))

    files_list = []
    if "files" in info:
        # Multi-file torrent
        for idx, f in enumerate(info["files"], start=1):
            path_parts = f.get("path.utf-8") or f.get("path", [])
            decoded_parts = [p.decode('utf-8', errors='replace') if isinstance(p, bytes) else str(p) for p in path_parts]
            rel_path = os.path.join(*decoded_parts) if decoded_parts else f"file_{idx}"
            folder = os.path.dirname(rel_path) if len(decoded_parts) > 1 else "Root"
            if not folder:
                folder = "Root"
            filename = decoded_parts[-1] if decoded_parts else f"file_{idx}"
            sz = int(f.get("length", 0))
            is_vid = filename.lower().endswith(MEDIA_EXTS)
            files_list.append({
                "index": idx,
                "rel_path": rel_path,
                "folder": folder,
                "filename": filename,
                "filename_clean": os.path.splitext(filename)[0],
                "size_bytes": sz,
                "size_str": format_torrent_size(sz),
                "is_video": is_vid,
                "label": f"[{format_torrent_size(sz)}] {filename}" + (f" ({folder})" if folder != "Root" else "")
            })
    else:
        # Single-file torrent
        sz = int(info.get("length", 0))
        is_vid = torrent_name.lower().endswith(MEDIA_EXTS)
        files_list.append({
            "index": 1,
            "rel_path": torrent_name,
            "folder": "Root",
            "filename": torrent_name,
            "filename_clean": os.path.splitext(torrent_name)[0],
            "size_bytes": sz,
            "size_str": format_torrent_size(sz),
            "is_video": is_vid,
            "label": f"[{format_torrent_size(sz)}] {torrent_name}"
        })

    folders = sorted(list(set(f["folder"] for f in files_list if f["is_video"] and f["folder"] != "Root")))
    total_sz = sum(f["size_bytes"] for f in files_list)
    video_count = sum(1 for f in files_list if f["is_video"])

    return {
        "name": torrent_name,
        "total_size_bytes": total_sz,
        "total_size_str": format_torrent_size(total_sz),
        "files": files_list,
        "folders": folders,
        "video_count": video_count
    }

def extract_file_path(file_obj: Any) -> Optional[str]:
    if isinstance(file_obj, str):
        return file_obj
    if hasattr(file_obj, 'name'):
        return file_obj.name
    if hasattr(file_obj, 'path'):
        return file_obj.path
    if isinstance(file_obj, dict):
        return file_obj.get('name') or file_obj.get('path')
    return None

def fetch_torrent_metadata(source: str) -> Dict[str, Any]:
    from config import TEMP_DIR
    meta_cache_dir = os.path.join(TEMP_DIR, "torrent_meta")
    os.makedirs(meta_cache_dir, exist_ok=True)

    # 1. Local .torrent File
    if os.path.isfile(source):
        with open(source, "rb") as f:
            data = f.read()
        res = parse_torrent_bytes(data)
        res["cached_file_path"] = source
        res["source_url"] = source
        return res

    # 2. Magnet Link
    if source.startswith("magnet:?"):
        xt_match = re.search(r'urn:btih:([a-zA-Z0-9]+)', source, re.I)
        if not xt_match:
            raise ValueError("Invalid magnet link: missing 'urn:btih:' hash.")
        btih = xt_match.group(1).upper()
        cache_path = os.path.join(meta_cache_dir, f"{btih}.torrent")

        if os.path.isfile(cache_path) and os.path.getsize(cache_path) > 100:
            with open(cache_path, "rb") as f:
                data = f.read()
            res = parse_torrent_bytes(data)
            res["cached_file_path"] = cache_path
            res["source_url"] = source
            return res

        # Attempt online torrent cache download (itorrents.org)
        itorrents_url = f"https://itorrents.org/torrent/{btih}.torrent"
        try:
            req = urllib.request.Request(itorrents_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=6) as resp:
                if resp.status == 200:
                    data = resp.read()
                    if b"d8:announce" in data or b"4:info" in data:
                        with open(cache_path, "wb") as f:
                            f.write(data)
                        res = parse_torrent_bytes(data)
                        res["cached_file_path"] = cache_path
                        res["source_url"] = source
                        return res
        except Exception:
            pass

        # Headless aria2c metadata retrieval fallback
        aria2_cmd = shutil.which("aria2c")
        if aria2_cmd:
            cmd = [
                aria2_cmd,
                "--bt-metadata-only=true",
                "--bt-save-metadata=true",
                f"--dir={meta_cache_dir}",
                "--bt-stop-timeout=25",
                "--summary-interval=0",
                source
            ]
            try:
                subprocess.run(cmd, capture_output=True, timeout=28)
                if os.path.isfile(cache_path) and os.path.getsize(cache_path) > 100:
                    with open(cache_path, "rb") as f:
                        data = f.read()
                    res = parse_torrent_bytes(data)
                    res["cached_file_path"] = cache_path
                    res["source_url"] = source
                    return res
            except Exception:
                pass

        # Fallback to display-name from magnet link if metadata cannot be fetched
        dn_match = re.search(r'[?&]dn=([^&]+)', source)
        raw_name = urllib.parse.unquote_plus(dn_match.group(1)) if dn_match else f"Torrent_{btih[:8]}"
        return {
            "name": raw_name,
            "total_size_bytes": 0,
            "total_size_str": "Unknown (Peer Swarm)",
            "files": [{
                "index": 1,
                "rel_path": raw_name,
                "folder": "Root",
                "filename": raw_name,
                "filename_clean": os.path.splitext(raw_name)[0],
                "size_bytes": 0,
                "size_str": "Unknown",
                "is_video": True,
                "label": f"[Swarm] {raw_name}"
            }],
            "folders": [],
            "video_count": 1,
            "cached_file_path": None,
            "source_url": source
        }

    raise ValueError(f"Unrecognized torrent source: {source}")
