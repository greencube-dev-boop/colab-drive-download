import re
import urllib.parse
import urllib.request
import json
from typing import List, Dict, Any, Tuple
from torrent_parser import format_torrent_size

PUBLIC_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.coppersurfer.tk:6969/announce",
    "udp://explodie.org:6969/announce"
]

def make_magnet(info_hash: str, name: str) -> str:
    encoded_name = urllib.parse.quote(name)
    tr_params = "&".join(f"tr={urllib.parse.quote(tr)}" for tr in PUBLIC_TRACKERS)
    return f"magnet:?xt=urn:btih:{info_hash}&dn={encoded_name}&{tr_params}"

def get_torrent_quality(title: str) -> str:
    t = title or ""
    if re.search(r'2160p|4K|UHD', t, re.I):
        return "4K"
    if re.search(r'1080p', t, re.I):
        return "1080p"
    if re.search(r'720p', t, re.I):
        return "720p"
    return "SD"

def query_apibay_torrents(query: str) -> List[Dict[str, Any]]:
    """Queries ThePirateBay's official JSON API (apibay.org)."""
    if not query:
        return []
    url = f"https://apibay.org/q.php?q={urllib.parse.quote(query)}"
    hits = []
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*'
        })
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            for item in data:
                if item.get("id") and item.get("id") != "0":
                    h = item.get("info_hash", "").strip()
                    name = item.get("name", "").strip()
                    seeds = int(item.get("seeders") or 0)
                    peers = int(item.get("leechers") or 0)
                    size = int(item.get("size") or 0)
                    if h and name:
                        hits.append({
                            "title": name,
                            "magnetUrl": make_magnet(h, name),
                            "seeds": seeds,
                            "peers": peers,
                            "bytes": size,
                            "source": "ThePirateBay"
                        })
    except Exception:
        pass
    return hits

def query_yts_lu_torrents(query: str, mode: str = "movie", year: Any = None) -> List[Dict[str, Any]]:
    """Queries en.yts.lu multi-tracker torrent API."""
    url = f"https://en.yts.lu/browse-movies?api=torrents&mode={mode}&name={urllib.parse.quote(query)}&quality=all"
    if year:
        url += f"&year={year}"
    hits = []
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://en.yts.lu/browse-movies',
            'Accept': 'application/json, text/plain, */*'
        })
        with urllib.request.urlopen(req, timeout=6) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode('utf-8'))
                for h in data.get("hits", []):
                    if h.get("magnetUrl"):
                        hits.append(h)
    except Exception:
        pass
    return hits

def search_live_torrents(title: str, year: Any = None, mode: str = "movie", season: Any = None) -> Tuple[List[str], Dict[str, Any]]:
    """
    Multi-engine swarm search combining The Pirate Bay (apibay.org) and en.yts.lu indexers.
    Returns quality-classified, health-sorted torrent options.
    """
    if not title:
        return [], {}

    queries = []
    if mode == "tv" and season:
        try:
            s_int = int(re.search(r'\d+', str(season)).group(0))
            queries.append(f"{title} S{s_int:02d}")
            queries.append(f"{title} Season {s_int}")
        except Exception:
            pass
    if year:
        queries.append(f"{title} {year}")
    queries.append(title)

    seen_magnets = set()
    raw_hits = []

    for q in queries:
        # 1. Query en.yts.lu indexer
        for h in query_yts_lu_torrents(q, mode=mode, year=year if q == title else None):
            mag = h.get("magnetUrl", "")
            if mag and mag not in seen_magnets:
                seen_magnets.add(mag)
                raw_hits.append(h)

        # 2. Query apibay.org (The Pirate Bay) indexer
        for h in query_apibay_torrents(q):
            mag = h.get("magnetUrl", "")
            if mag and mag not in seen_magnets:
                seen_magnets.add(mag)
                raw_hits.append(h)

        if len(raw_hits) >= 30:
            break

    filtered_hits = []
    for h in raw_hits:
        t_title = h.get("title", "")
        if mode == "tv" and season:
            try:
                s_num = int(re.search(r'\d+', str(season)).group(0))
                s_pad = f"{s_num:02d}"
                s_pattern = rf'(S{s_pad}|S{s_num}[^0-9]|SEASON[.\s-_]*{s_num}|COMPLETE|ALL.?SEASONS|COLLECTION)'
                if not re.search(s_pattern, t_title, re.I):
                    continue
            except Exception:
                pass
        filtered_hits.append(h)

    # Sort primarily by seeds (descending), secondarily by file size
    filtered_hits.sort(key=lambda x: (int(x.get("seeds") or 0), int(x.get("bytes") or 0)), reverse=True)

    formatted_choices = []
    torrent_map = {}
    for h in filtered_hits[:35]:
        t_title = h.get("title", "Unknown")
        seeds = int(h.get("seeds") or 0)
        peers = int(h.get("peers") or 0)
        sz_str = format_torrent_size(int(h.get("bytes") or 0))
        q_label = get_torrent_quality(t_title)
        source = h.get("source", "Index")
        mag = h.get("magnetUrl", "")

        label = f"[{q_label}] {t_title} | 📦 {sz_str} | 🔥 {seeds} seeds ({peers} peers) [{source}]"
        formatted_choices.append(label)
        torrent_map[label] = {
            "title": t_title,
            "magnet": mag,
            "seeds": seeds,
            "peers": peers,
            "size_str": sz_str,
            "quality": q_label,
            "source": source
        }

    return formatted_choices, torrent_map
