import re
import urllib.parse
import urllib.request
import json
from typing import List, Dict, Any, Tuple
from torrent_parser import format_torrent_size

def get_torrent_quality(title: str) -> str:
    t = title or ""
    if re.search(r'2160p|4K|UHD', t, re.I):
        return "4K"
    if re.search(r'1080p', t, re.I):
        return "1080p"
    if re.search(r'720p', t, re.I):
        return "720p"
    return "SD"

def search_live_torrents(title: str, year: Any = None, mode: str = "movie", season: Any = None) -> Tuple[List[str], Dict[str, Any]]:
    """
    Queries the en.yts.lu multi-tracker torrent API (The Pirate Bay, YTS, 1337x)
    and returns quality-classified, health-sorted torrent options.
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
        url = f"https://en.yts.lu/browse-movies?api=torrents&mode={mode}&name={urllib.parse.quote(q)}&quality=all"
        if year and q == title:
            url += f"&year={year}"
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://en.yts.lu/browse-movies',
                'Accept': 'application/json, text/plain, */*'
            })
            with urllib.request.urlopen(req, timeout=8) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode('utf-8'))
                    hits = data.get("hits", [])
                    for h in hits:
                        mag = h.get("magnetUrl", "")
                        if mag and mag not in seen_magnets:
                            seen_magnets.add(mag)
                            raw_hits.append(h)
        except Exception:
            pass
        if len(raw_hits) >= 15:
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

    filtered_hits.sort(key=lambda x: (int(x.get("seeds") or 0), int(x.get("bytes") or 0)), reverse=True)

    formatted_choices = []
    torrent_map = {}
    for h in filtered_hits[:25]:
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
