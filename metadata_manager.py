import os
import re
import glob
import json
import shutil
import datetime
import threading
from typing import Dict, Any, List, Optional, Tuple
import config

_meta_lock = threading.RLock()

def normalize_title(t: str) -> str:
    """Normalizes title string for duplicate comparison (removes 'the', punctuation, whitespace)."""
    if not t:
        return ""
    s = t.lower().strip()
    s = re.sub(r'^(the|a|an)\s+', '', s)
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def is_same_media(m1: Dict[str, Any], m2: Dict[str, Any]) -> bool:
    """Compares two classified media dictionaries to determine if they represent the exact same media item."""
    if not m1 or not m2:
        return False
    t1 = m1.get("type", "other").lower()
    t2 = m2.get("type", "other").lower()

    if t1 in ["tv_series", "tv", "tvseris"] and t2 in ["tv_series", "tv", "tvseris"]:
        s1 = normalize_title(m1.get("show_name") or m1.get("title", ""))
        s2 = normalize_title(m2.get("show_name") or m2.get("title", ""))
        season1 = int(m1.get("season", 0) or 0)
        season2 = int(m2.get("season", 0) or 0)
        ep1 = int(m1.get("episode", 0) or 0)
        ep2 = int(m2.get("episode", 0) or 0)
        if s1 == s2 and season1 == season2 and ep1 == ep2 and season1 > 0 and ep1 > 0:
            return True
        return False

    elif t1 == "movie" and t2 == "movie":
        name1 = normalize_title(m1.get("title", ""))
        name2 = normalize_title(m2.get("title", ""))
        y1 = m1.get("year")
        y2 = m2.get("year")
        if name1 == name2:
            if y1 and y2:
                return int(y1) == int(y2)
            return True
        return False

    else:
        name1 = normalize_title(m1.get("title", "") or m1.get("formatted_name", ""))
        name2 = normalize_title(m2.get("title", "") or m2.get("formatted_name", ""))
        return bool(name1 and name1 == name2)

def _default_schema() -> Dict[str, Any]:
    return {
        "movies": [],
        "tvseris": [],
        "other": []
    }

def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def load_metadata() -> Dict[str, Any]:
    """Loads metadata.json thread-safely with automatic initialization."""
    with _meta_lock:
        if not os.path.exists(config.METADATA_JSON):
            data = _default_schema()
            save_metadata(data)
            return data
        try:
            with open(config.METADATA_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    data = _default_schema()
                data.setdefault("movies", [])
                if "tv_series" in data and "tvseris" not in data:
                    data["tvseris"] = data["tv_series"]
                data.setdefault("tvseris", [])
                data.setdefault("other", [])
                return data
        except Exception as e:
            print(f"[Metadata] Warning loading metadata: {e}. Initializing default.")
            return _default_schema()

def save_metadata(data: Dict[str, Any]):
    """Saves metadata to disk safely using atomic temp file replacement."""
    os.makedirs(config.DATA_DIR, exist_ok=True)
    temp_file = config.METADATA_JSON + ".tmp"
    if "tvseris" in data:
        data["tv_series"] = data["tvseris"]
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        try:
            if os.path.exists(config.METADATA_JSON):
                os.replace(temp_file, config.METADATA_JSON)
            else:
                os.rename(temp_file, config.METADATA_JSON)
        except OSError:
            shutil.copy2(temp_file, config.METADATA_JSON)
            if os.path.exists(temp_file):
                os.remove(temp_file)
    except Exception as e:
        print(f"[Metadata] Error saving metadata: {e}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception:
            pass

def check_media_exists(media_info: Dict[str, Any]) -> Tuple[bool, str, str]:
    """
    Checks if a media item is already present in Google Drive metadata or filesystem.
    Returns: (exists: bool, status: 'raw'|'refined'|'disk', path: str)
    """
    if not media_info:
        return False, "", ""

    m_type = media_info.get("type", "other").lower()
    data = load_metadata()

    # 1. Check TV Series in metadata
    if m_type in ["tv_series", "tv", "tvseris"]:
        target_show = normalize_title(media_info.get("show_name") or media_info.get("title", ""))
        season = int(media_info.get("season", 0) or 0)
        episode = int(media_info.get("episode", 0) or 0)
        season_key = f"season_{season:02d}"
        ep_key = f"episode_{episode:02d}"

        if season > 0 and episode > 0:
            for s in data.get("tvseris", []):
                s_name = normalize_title(s.get("name", ""))
                if s_name == target_show:
                    ep_data = s.get("seasons", {}).get(season_key, {}).get(ep_key)
                    if ep_data:
                        st = ep_data.get("status", "raw")
                        p = ep_data.get("refined_path") or ep_data.get("raw_path") or ""
                        return True, st, p

            # Check physical filesystem in Drive
            pat_refined = os.path.join(config.REFINED_TV_DIR, "*", f"Season {season:02d}", f"*S{season:02d}E{episode:02d}*.*")
            matches = glob.glob(pat_refined)
            if matches:
                return True, "refined", matches[0]

            pat_raw = os.path.join(config.RAW_DIR, f"*S{season:02d}E{episode:02d}*.*")
            matches_raw = glob.glob(pat_raw)
            if matches_raw:
                return True, "raw", matches_raw[0]

    # 2. Check Movies in metadata
    elif m_type == "movie":
        target_name = normalize_title(media_info.get("title", ""))
        target_year = media_info.get("year")

        for m in data.get("movies", []):
            m_name = normalize_title(m.get("name", ""))
            if target_name in m_name:
                if target_year and str(target_year) not in m_name:
                    continue
                st = m.get("status", "raw")
                p = m.get("refined_path") or m.get("raw_path") or ""
                return True, st, p

        # Check physical filesystem in Drive
        clean_stem = config.sanitize_filename(media_info.get("formatted_name", ""))
        matches_refined = glob.glob(os.path.join(config.REFINED_MOVIES_DIR, f"{clean_stem}.*"))
        if matches_refined:
            return True, "refined", matches_refined[0]

        matches_raw = glob.glob(os.path.join(config.RAW_DIR, f"{clean_stem}.*"))
        if matches_raw:
            return True, "raw", matches_raw[0]

    # 3. Check Other in metadata
    else:
        target_title = normalize_title(media_info.get("title", "") or media_info.get("formatted_name", ""))
        for o in data.get("other", []):
            if normalize_title(o.get("name", "")) == target_title:
                st = o.get("status", "raw")
                p = o.get("refined_path") or o.get("raw_path") or ""
                return True, st, p

    return False, "", ""

def prune_pending_duplicates(media_info: Dict[str, Any]) -> int:
    """
    Searches pending items in the download queue and cancels any that match the newly added media.
    Prevents duplicate downloads from running when another source already completed the download.
    """
    import database
    cancelled_count = 0
    try:
        dl_items = database.get_download_queue()
        for item in dl_items:
            if item.get("status") == "pending":
                import classifier
                item_info = classifier.classify_media(item.get("title", ""))
                if is_same_media(media_info, item_info):
                    i_id = item["id"]
                    msg = f"Cancelled: Duplicate of existing media ({media_info.get('formatted_name')})"
                    database.update_item_status("download_queue", i_id, status="cancelled", error_msg=msg)
                    # Also cancel in-memory if present
                    with config.tasks_lock:
                        for t in config.tasks:
                            if t.get("db_id") == i_id or t.get("id") == i_id:
                                t["status"] = "Cancelled"
                                t["cancelled"] = True
                    cancelled_count += 1
                    print(f"[Deduplication] Automatically cancelled pending duplicate download #{i_id}: '{item.get('title')}'")
    except Exception as e:
        print(f"[Deduplication] Notice during pending prune: {e}")

    return cancelled_count

def record_item(
    media_type: str,
    name: str,
    status: str = "raw",
    raw_path: str = "",
    refined_path: str = "",
    refining_data: Optional[Dict[str, Any]] = None,
    show_name: str = "",
    season: int = 0,
    episode: int = 0
):
    """
    Records or updates a media entry in metadata.json matching user's schema:
    - movies: [{name, status, raw_path, refined_path, refining_data}]
    - tvseris: [{name: show_name, seasons: {season_n: {episode_n: {status, refining_data}}}}]
    - other: [{name, status, raw_path, refined_path, refining_data}]
    """
    if refining_data is None:
        refining_data = {}

    with _meta_lock:
        data = load_metadata()
        m_type = (media_type or "other").lower()
        now = _now_iso()

        if m_type == "movie":
            movies_list: List[Dict[str, Any]] = data.get("movies", [])
            existing = next((m for m in movies_list if m.get("name") == name), None)
            if existing:
                existing["status"] = status
                if raw_path: existing["raw_path"] = raw_path
                if refined_path: existing["refined_path"] = refined_path
                existing["refining_data"] = refining_data
                existing["updated_at"] = now
            else:
                movies_list.append({
                    "name": name,
                    "status": status,
                    "raw_path": raw_path,
                    "refined_path": refined_path,
                    "refining_data": refining_data,
                    "created_at": now,
                    "updated_at": now
                })
            data["movies"] = movies_list

        elif m_type in ["tv_series", "tvseris", "tv"]:
            s_name = show_name or name
            season_key = f"season_{season:02d}" if season > 0 else "season_01"
            ep_key = f"episode_{episode:02d}" if episode > 0 else "episode_01"

            tv_list: List[Dict[str, Any]] = data.get("tvseris", [])
            existing_show = next((s for s in tv_list if s.get("name", "").lower() == s_name.lower()), None)

            ep_data = {
                "name": name,
                "status": status,
                "raw_path": raw_path,
                "refined_path": refined_path,
                "refining_data": refining_data,
                "updated_at": now
            }

            if not existing_show:
                existing_show = {
                    "name": s_name,
                    "seasons": {
                        season_key: {
                            ep_key: ep_data
                        }
                    },
                    "created_at": now,
                    "updated_at": now
                }
                tv_list.append(existing_show)
            else:
                seasons = existing_show.setdefault("seasons", {})
                episodes = seasons.setdefault(season_key, {})
                episodes[ep_key] = ep_data
                existing_show["updated_at"] = now

            data["tvseris"] = tv_list

        else:
            other_list: List[Dict[str, Any]] = data.get("other", [])
            existing = next((o for o in other_list if o.get("name") == name), None)
            if existing:
                existing["status"] = status
                if raw_path: existing["raw_path"] = raw_path
                if refined_path: existing["refined_path"] = refined_path
                existing["refining_data"] = refining_data
                existing["updated_at"] = now
            else:
                other_list.append({
                    "name": name,
                    "status": status,
                    "raw_path": raw_path,
                    "refined_path": refined_path,
                    "refining_data": refining_data,
                    "created_at": now,
                    "updated_at": now
                })
            data["other"] = other_list

        save_metadata(data)

def update_item_status(
    media_type: str,
    name: str,
    status: str,
    refined_path: str = "",
    show_name: str = "",
    season: int = 0,
    episode: int = 0
):
    """Updates status and refined_path for an existing entry."""
    with _meta_lock:
        data = load_metadata()
        m_type = (media_type or "other").lower()
        now = _now_iso()

        if m_type == "movie":
            for m in data.get("movies", []):
                if m.get("name") == name:
                    m["status"] = status
                    if refined_path: m["refined_path"] = refined_path
                    m["updated_at"] = now
                    break

        elif m_type in ["tv_series", "tvseris", "tv"]:
            s_name = show_name or name
            season_key = f"season_{season:02d}" if season > 0 else "season_01"
            ep_key = f"episode_{episode:02d}" if episode > 0 else "episode_01"

            for s in data.get("tvseris", []):
                if s.get("name", "").lower() == s_name.lower():
                    ep = s.get("seasons", {}).get(season_key, {}).get(ep_key)
                    if ep:
                        ep["status"] = status
                        if refined_path: ep["refined_path"] = refined_path
                        ep["updated_at"] = now
                    break

        else:
            for o in data.get("other", []):
                if o.get("name") == name:
                    o["status"] = status
                    if refined_path: o["refined_path"] = refined_path
                    o["updated_at"] = now
                    break

        save_metadata(data)
