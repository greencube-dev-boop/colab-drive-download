import os
import json
import datetime
import threading
from typing import Dict, Any, List, Optional
import config

_meta_lock = threading.RLock()

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
                # Support both tvseris and tv_series
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
    # Ensure tv_series is mirrored for convenience
    if "tvseris" in data:
        data["tv_series"] = data["tvseris"]
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        if os.path.exists(config.METADATA_JSON):
            os.replace(temp_file, config.METADATA_JSON)
        else:
            os.rename(temp_file, config.METADATA_JSON)
    except Exception as e:
        print(f"[Metadata] Error saving metadata: {e}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception:
            pass

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
