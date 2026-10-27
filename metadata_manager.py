import os
import re
import glob
import json
import time
import shutil
import datetime
import threading
from typing import Dict, Any, List, Optional, Tuple
import config

_meta_lock = threading.RLock()
_cached_metadata: Optional[Dict[str, Any]] = None
_cached_file: str = ""
_last_cache_time: float = 0.0

def reset_cache():
    """Resets in-memory metadata cache."""
    global _cached_metadata, _cached_file, _last_cache_time
    with _meta_lock:
        _cached_metadata = None
        _cached_file = ""
        _last_cache_time = 0.0

def normalize_title(t: str) -> str:
    """Normalizes title string for duplicate comparison (removes 'the', punctuation, whitespace)."""
    if not t:
        return ""
    s = t.lower().strip()
    s = re.sub(r'^(the|a|an)\s+', '', s)
    s = re.sub(r'[,_.-]+\s*(the|a|an)\b', '', s)
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def _is_show_match(s1: str, s2: str) -> bool:
    """Accurately compares two show names ignoring case, punctuation, articles, and release years."""
    norm1 = normalize_title(s1)
    norm2 = normalize_title(s2)
    if not norm1 or not norm2:
        return False
    if norm1 == norm2:
        return True
    w1 = re.sub(r'\b(19\d\d|20\d\d)\b', '', norm1).strip()
    w2 = re.sub(r'\b(19\d\d|20\d\d)\b', '', norm2).strip()
    w1 = re.sub(r'\s+', ' ', w1).strip()
    w2 = re.sub(r'\s+', ' ', w2).strip()
    return bool(w1 and w1 == w2)

def _is_movie_match(title1: str, year1: Optional[int], title2: str, year2: Optional[int]) -> bool:
    """Compares two movie titles and years with strict precision."""
    n1 = normalize_title(title1)
    n2 = normalize_title(title2)
    if not n1 or not n2:
        return False

    # Infer years from titles if not provided
    if not year1:
        y1_match = re.search(r'\b(19\d\d|20\d\d)\b', n1)
        if y1_match:
            year1 = int(y1_match.group(1))
    if not year2:
        y2_match = re.search(r'\b(19\d\d|20\d\d)\b', n2)
        if y2_match:
            year2 = int(y2_match.group(1))

    # If both have years and they differ, they are definitely different movies
    if year1 and year2 and int(year1) != int(year2):
        return False

    if n1 == n2:
        return True

    w1 = re.sub(r'\b(19\d\d|20\d\d)\b', '', n1).strip()
    w2 = re.sub(r'\b(19\d\d|20\d\d)\b', '', n2).strip()
    w1 = re.sub(r'\s+', ' ', w1).strip()
    w2 = re.sub(r'\s+', ' ', w2).strip()
    return bool(w1 and w1 == w2)

def is_same_media(m1: Dict[str, Any], m2: Dict[str, Any]) -> bool:
    """Compares two classified media dictionaries to determine if they represent the exact same media item."""
    if not m1 or not m2:
        return False
    t1 = m1.get("type", "other").lower()
    t2 = m2.get("type", "other").lower()

    if t1 in ["tv_series", "tv", "tvseris"] and t2 in ["tv_series", "tv", "tvseris"]:
        s1 = m1.get("show_name") or m1.get("title", "")
        s2 = m2.get("show_name") or m2.get("title", "")
        season1 = int(m1.get("season", 0) or 0)
        season2 = int(m2.get("season", 0) or 0)
        ep1 = int(m1.get("episode", 0) or 0)
        ep2 = int(m2.get("episode", 0) or 0)
        if _is_show_match(s1, s2) and season1 == season2 and ep1 == ep2 and season1 > 0 and ep1 > 0:
            return True
        return False

    elif (t1 == "movie" or t2 == "movie") and (t1 not in ["tv_series", "tv", "tvseris"] and t2 not in ["tv_series", "tv", "tvseris"]):
        return _is_movie_match(
            m1.get("title", "") or m1.get("formatted_name", ""),
            m1.get("year"),
            m2.get("title", "") or m2.get("formatted_name", ""),
            m2.get("year")
        )

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

def load_metadata(force_reload: bool = False) -> Dict[str, Any]:
    """Loads metadata thread-safely with caching and local SSD mirroring."""
    global _cached_metadata, _cached_file, _last_cache_time
    with _meta_lock:
        now = time.time()
        local_meta = getattr(config, "LOCAL_METADATA_JSON", os.path.join(config.DATA_DIR, "metadata.json"))
        drive_meta = getattr(config, "METADATA_JSON", os.path.join(config.DATA_DIR, "metadata.json"))

        # In Colab, initialize local SSD working copy from Drive if available
        if getattr(config, "IS_COLAB", False) and os.path.exists(drive_meta) and not os.path.exists(local_meta):
            try:
                os.makedirs(os.path.dirname(local_meta), exist_ok=True)
                shutil.copyfile(drive_meta, local_meta)
            except Exception as e:
                print(f"[Metadata] Notice: Initial copy from Drive: {e}")

        target_file = local_meta if (getattr(config, "IS_COLAB", False) and os.path.exists(local_meta)) else drive_meta

        if not force_reload and _cached_metadata is not None and _cached_file == target_file and (now - _last_cache_time) < 5.0:
            return _cached_metadata

        if not os.path.exists(target_file):
            data = _default_schema()
            save_metadata(data)
            _cached_metadata = data
            _cached_file = target_file
            _last_cache_time = now
            return data
        try:
            with open(target_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    data = _default_schema()
                data.setdefault("movies", [])
                if "tv_series" in data and "tvseris" not in data:
                    data["tvseris"] = data["tv_series"]
                data.setdefault("tvseris", [])
                data.setdefault("other", [])
                _cached_metadata = data
                _cached_file = target_file
                _last_cache_time = now
                return data
        except Exception as e:
            print(f"[Metadata] Warning loading metadata: {e}. Initializing default.")
            data = _default_schema()
            _cached_metadata = data
            _cached_file = target_file
            _last_cache_time = now
            return data

def save_metadata(data: Dict[str, Any]):
    """
    Saves metadata safely using local SSD atomic write and single-shot Drive mirroring.
    Completely avoids writing .tmp files to Google Drive FUSE to prevent ghost / duplicate files.
    """
    global _cached_metadata, _cached_file, _last_cache_time
    with _meta_lock:
        if "tvseris" in data:
            data["tv_series"] = data["tvseris"]

        local_dir = getattr(config, "LOCAL_DATA_DIR", config.DATA_DIR)
        os.makedirs(local_dir, exist_ok=True)
        local_meta = getattr(config, "LOCAL_METADATA_JSON", os.path.join(local_dir, "metadata.json"))
        local_temp = os.path.join(local_dir, "metadata_atomic.tmp")
        target_file = local_meta if getattr(config, "IS_COLAB", False) else getattr(config, "METADATA_JSON", local_meta)

        _cached_metadata = data
        _cached_file = target_file
        _last_cache_time = time.time()

        try:
            # 1. Atomic write on local SSD
            with open(local_temp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            try:
                os.replace(local_temp, local_meta)
            except OSError:
                shutil.copyfile(local_temp, local_meta)
                if os.path.exists(local_temp):
                    os.remove(local_temp)

            # 2. Single-pass mirror to Google Drive (never write .tmp on Drive FUSE!)
            drive_meta = getattr(config, "METADATA_JSON", None)
            if getattr(config, "IS_COLAB", False) and drive_meta:
                drive_meta_dir = os.path.abspath(os.path.dirname(drive_meta))
                forbidden_roots = [
                    os.path.abspath("/content/drive/MyDrive"),
                    os.path.abspath("/content/drive"),
                    os.path.abspath("/content"),
                    os.path.abspath("/")
                ]
                if drive_meta_dir not in forbidden_roots:
                    os.makedirs(drive_meta_dir, exist_ok=True)
                    shutil.copyfile(local_meta, drive_meta)
                else:
                    print(f"[Metadata] Security Warning: Refusing to mirror metadata directly into Drive root ({drive_meta_dir}).")
            elif not getattr(config, "IS_COLAB", False) and drive_meta and drive_meta != local_meta:
                os.makedirs(os.path.dirname(drive_meta), exist_ok=True)
                shutil.copyfile(local_meta, drive_meta)

        except Exception as e:
            print(f"[Metadata] Error saving metadata: {e}")
            if os.path.exists(local_temp):
                try:
                    os.remove(local_temp)
                except Exception:
                    pass

def check_media_exists(media_info: Dict[str, Any]) -> Tuple[bool, str, str]:
    """
    Checks if a media item is already present in Google Drive metadata or filesystem.
    Returns: (exists: bool, status: 'raw'|'refined'|'disk', path: str)
    Strictly isolates TV series by show name so different shows with the same
    season/episode number (e.g. S01E06) never collide.
    """
    if not media_info:
        return False, "", ""

    m_type = media_info.get("type", "other").lower()
    data = load_metadata()

    # 1. Check TV Series
    if m_type in ["tv_series", "tv", "tvseris"]:
        raw_show = media_info.get("show_name") or media_info.get("title", "")
        season = int(media_info.get("season", 0) or 0)
        episode = int(media_info.get("episode", 0) or 0)
        season_key = f"season_{season:02d}"
        ep_key = f"episode_{episode:02d}"

        if season > 0 and episode > 0 and raw_show:
            # Step A: Check metadata.json (exact show match via _is_show_match)
            for s in data.get("tvseris", []):
                s_name = s.get("name", "")
                if _is_show_match(s_name, raw_show):
                    ep_data = s.get("seasons", {}).get(season_key, {}).get(ep_key)
                    if ep_data:
                        st = ep_data.get("status", "raw")
                        p = ep_data.get("refined_path") or ep_data.get("raw_path") or ""
                        return True, st, p

            # Step B: Check physical refined filesystem in Drive:
            # ONLY look in folders under REFINED_TV_DIR that strictly match raw_show!
            if os.path.exists(config.REFINED_TV_DIR):
                try:
                    for show_dir in os.listdir(config.REFINED_TV_DIR):
                        show_dir_path = os.path.join(config.REFINED_TV_DIR, show_dir)
                        if not os.path.isdir(show_dir_path):
                            continue
                        if _is_show_match(show_dir, raw_show):
                            season_path = os.path.join(show_dir_path, f"Season {season:02d}")
                            if os.path.exists(season_path):
                                pat_refined = os.path.join(season_path, f"*S{season:02d}E{episode:02d}*.*")
                                matches = glob.glob(pat_refined)
                                if matches:
                                    return True, "refined", matches[0]
                except Exception as e:
                    print(f"[Metadata] Warning checking refined TV filesystem: {e}")

            # Step C: Check physical raw filesystem in Drive:
            # Files in RAW_DIR are flat. Candidate files matching SxxExx MUST match raw_show!
            if os.path.exists(config.RAW_DIR):
                try:
                    pat_raw = os.path.join(config.RAW_DIR, f"*S{season:02d}E{episode:02d}*.*")
                    matches_raw = glob.glob(pat_raw)
                    for rf in matches_raw:
                        rf_base = os.path.basename(rf)
                        import classifier
                        rf_info = classifier.heuristic_classify(rf_base)
                        if is_same_media(media_info, rf_info):
                            return True, "raw", rf
                except Exception as e:
                    print(f"[Metadata] Warning checking raw TV filesystem: {e}")

    # 2. Check Movies
    elif m_type == "movie":
        target_name = media_info.get("title", "")
        target_year = media_info.get("year")

        # Step A: Check metadata.json
        for m in data.get("movies", []):
            m_title = m.get("name", "")
            if _is_movie_match(target_name, target_year, m_title, m.get("year")):
                st = m.get("status", "raw")
                p = m.get("refined_path") or m.get("raw_path") or ""
                return True, st, p

        # Step B: Check physical refined filesystem in Drive
        if os.path.exists(config.REFINED_MOVIES_DIR):
            try:
                for mf in os.listdir(config.REFINED_MOVIES_DIR):
                    mf_path = os.path.join(config.REFINED_MOVIES_DIR, mf)
                    if not os.path.isfile(mf_path):
                        continue
                    import classifier
                    mf_info = classifier.heuristic_classify(mf)
                    if is_same_media(media_info, mf_info):
                        return True, "refined", mf_path
            except Exception as e:
                print(f"[Metadata] Warning checking refined movie filesystem: {e}")

        # Step C: Check physical raw filesystem in Drive
        if os.path.exists(config.RAW_DIR):
            try:
                clean_stem = config.sanitize_filename(media_info.get("formatted_name", ""))
                if clean_stem:
                    matches_raw = glob.glob(os.path.join(config.RAW_DIR, f"{clean_stem}.*"))
                    if matches_raw:
                        return True, "raw", matches_raw[0]

                # If raw filename differed from formatted_name (e.g. scene release dots/tags), classify raw candidates
                if target_year:
                    candidates = glob.glob(os.path.join(config.RAW_DIR, f"*{target_year}*.*"))
                else:
                    candidates = [os.path.join(config.RAW_DIR, f) for f in os.listdir(config.RAW_DIR)]

                for rf in candidates:
                    if not os.path.isfile(rf):
                        continue
                    rf_base = os.path.basename(rf)
                    import classifier
                    rf_info = classifier.heuristic_classify(rf_base)
                    if is_same_media(media_info, rf_info):
                        return True, "raw", rf
            except Exception as e:
                print(f"[Metadata] Warning checking raw movie filesystem: {e}")

    # 3. Check Other in metadata
    else:
        target_title = normalize_title(media_info.get("title", "") or media_info.get("formatted_name", ""))
        if target_title:
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
    episode: int = 0,
    year: Optional[int] = None
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
            if year is None:
                y_match = re.search(r'\b(19\d\d|20\d\d)\b', name)
                if y_match:
                    year = int(y_match.group(1))

            movies_list: List[Dict[str, Any]] = data.get("movies", [])
            existing = next((m for m in movies_list if _is_movie_match(m.get("name", ""), m.get("year"), name, year)), None)
            if existing:
                existing["status"] = status
                if raw_path: existing["raw_path"] = raw_path
                if refined_path: existing["refined_path"] = refined_path
                existing["refining_data"] = refining_data
                if year and not existing.get("year"):
                    existing["year"] = year
                existing["updated_at"] = now
            else:
                movies_list.append({
                    "name": name,
                    "year": year,
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
            existing_show = next((s for s in tv_list if _is_show_match(s.get("name", ""), s_name)), None)

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
    episode: int = 0,
    year: Optional[int] = None
):
    """Updates status and refined_path for an existing entry."""
    with _meta_lock:
        data = load_metadata()
        m_type = (media_type or "other").lower()
        now = _now_iso()

        if m_type == "movie":
            if year is None:
                y_match = re.search(r'\b(19\d\d|20\d\d)\b', name)
                if y_match:
                    year = int(y_match.group(1))

            for m in data.get("movies", []):
                if _is_movie_match(m.get("name", ""), m.get("year"), name, year):
                    m["status"] = status
                    if refined_path: m["refined_path"] = refined_path
                    if year and not m.get("year"):
                        m["year"] = year
                    m["updated_at"] = now
                    break

        elif m_type in ["tv_series", "tvseris", "tv"]:
            s_name = show_name or name
            season_key = f"season_{season:02d}" if season > 0 else "season_01"
            ep_key = f"episode_{episode:02d}" if episode > 0 else "episode_01"

            for s in data.get("tvseris", []):
                if _is_show_match(s.get("name", ""), s_name):
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
