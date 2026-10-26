import os
import time
import json
import shutil
import sqlite3
import datetime
import threading
from typing import Dict, Any, List, Optional, Tuple
import config

_db_lock = threading.Lock()
_last_db_sync_time: float = 0.0

def sync_db_to_drive(force: bool = False):
    """
    Safely mirrors local SQLite database to Google Drive for multi-session persistence.
    Uses local SSD staging snapshot and single-pass direct copy to Drive FUSE,
    completely eliminating ghost / duplicate .tmp files on Google Drive.
    Debounces high-frequency writes (minimum 5s interval unless force=True).
    """
    global _last_db_sync_time
    if not getattr(config, "IS_COLAB", False):
        return
    drive_db = getattr(config, "DRIVE_QUEUE_DB", None)
    local_db = getattr(config, "LOCAL_QUEUE_DB", None)
    if not drive_db or not local_db or not os.path.exists(local_db):
        return

    # Safety check: Drive DB MUST NOT be directly in MyDrive or Drive root
    drive_db_dir = os.path.abspath(os.path.dirname(drive_db))
    forbidden_roots = [
        os.path.abspath("/content/drive/MyDrive"),
        os.path.abspath("/content/drive"),
        os.path.abspath("/content"),
        os.path.abspath("/")
    ]
    if drive_db_dir in forbidden_roots:
        print(f"[Database] Security Warning: Refusing to sync DB directly into Drive root ({drive_db_dir}). Must be inside a subfolder.")
        return

    now = time.time()
    if not force and (now - _last_db_sync_time) < 5.0:
        return
    _last_db_sync_time = now

    try:
        os.makedirs(drive_db_dir, exist_ok=True)
        # Use local SSD for the temporary snapshot, never write .tmp on Drive FUSE!
        local_sync_tmp = local_db + ".sync_tmp"
        shutil.copyfile(local_db, local_sync_tmp)
        # Single-pass direct file copy to Drive destination
        shutil.copyfile(local_sync_tmp, drive_db)
        if os.path.exists(local_sync_tmp):
            try:
                os.remove(local_sync_tmp)
            except Exception:
                pass
    except Exception as e:
        # Gracefully handle shared viewer / read-only drive mounts
        pass

def _commit_and_sync(conn: sqlite3.Connection, force: bool = False):
    conn.commit()
    sync_db_to_drive(force=force)

def _get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(config.QUEUE_DB), exist_ok=True)
    if getattr(config, "IS_COLAB", False):
        drive_db = getattr(config, "DRIVE_QUEUE_DB", None)
        local_db = getattr(config, "LOCAL_QUEUE_DB", None)
        if drive_db and local_db and os.path.exists(drive_db) and not os.path.exists(local_db):
            try:
                os.makedirs(os.path.dirname(local_db), exist_ok=True)
                shutil.copyfile(drive_db, local_db)
            except Exception as e:
                print(f"[Database] Notice: Initial copy from Drive: {e}")

    conn = sqlite3.connect(config.QUEUE_DB, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA cache_size = -64000;")
        conn.execute("PRAGMA busy_timeout = 30000;")
    except Exception:
        pass
    return conn

def init_db():
    """Initializes SQLite schema for persistent download and refining queues."""
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS download_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                source_type TEXT DEFAULT 'direct',
                status TEXT DEFAULT 'pending',
                order_idx INTEGER DEFAULT 0,
                download_pct REAL DEFAULT 0.0,
                download_speed TEXT DEFAULT '--',
                raw_path TEXT DEFAULT '',
                refining_config TEXT DEFAULT '{}',
                error_msg TEXT DEFAULT '',
                is_low_speed INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)

            # Dynamic schema migration for existing databases
            cur.execute("PRAGMA table_info(download_queue)")
            cols = [r["name"] for r in cur.fetchall()]
            if "is_low_speed" not in cols:
                cur.execute("ALTER TABLE download_queue ADD COLUMN is_low_speed INTEGER DEFAULT 0")
            if "retry_count" not in cols:
                cur.execute("ALTER TABLE download_queue ADD COLUMN retry_count INTEGER DEFAULT 0")

            cur.execute("""
            CREATE TABLE IF NOT EXISTS refine_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                media_type TEXT DEFAULT 'other',
                show_name TEXT DEFAULT '',
                season INTEGER DEFAULT 0,
                episode INTEGER DEFAULT 0,
                title TEXT NOT NULL,
                raw_drive_path TEXT NOT NULL,
                target_refined_path TEXT DEFAULT '',
                status TEXT DEFAULT 'pending',
                order_idx INTEGER DEFAULT 0,
                refining_config TEXT DEFAULT '{}',
                convert_pct REAL DEFAULT 0.0,
                raw_size_mb REAL DEFAULT 0.0,
                final_size_mb REAL DEFAULT 0.0,
                chunk_status TEXT DEFAULT '{}',
                error_msg TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)

            cur.execute("CREATE INDEX IF NOT EXISTS idx_dl_status ON download_queue(status, order_idx);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_dl_speed_status ON download_queue(is_low_speed, status, order_idx);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_rf_status ON refine_queue(status, order_idx);")
            _commit_and_sync(conn, force=True)
        finally:
            conn.close()

def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def enqueue_download(item: Dict[str, Any]) -> int:
    """Inserts a new item into download_queue with next available order_idx for its speed tier."""
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            is_low = int(item.get("is_low_speed", 0))
            retry_count = int(item.get("retry_count", 0))
            cur.execute("SELECT COALESCE(MAX(order_idx), 0) + 1 FROM download_queue WHERE status = 'pending' AND is_low_speed = ?", (is_low,))
            next_idx = cur.fetchone()[0]

            now = _now_iso()
            refining_cfg = json.dumps(item.get("refining_config", {}))
            cur.execute("""
                INSERT INTO download_queue 
                (title, url, source_type, status, order_idx, download_pct, download_speed, raw_path, refining_config, error_msg, is_low_speed, retry_count, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, 0.0, '--', '', ?, '', ?, ?, ?, ?)
            """, (
                item.get("title", "Untitled Download"),
                item.get("url", ""),
                item.get("source_type", "direct"),
                next_idx,
                refining_cfg,
                is_low,
                retry_count,
                now,
                now
            ))
            _commit_and_sync(conn, force=True)
            return cur.lastrowid
        finally:
            conn.close()

def enqueue_refine(item: Dict[str, Any]) -> int:
    """Inserts a new item into refine_queue with next available order_idx."""
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COALESCE(MAX(order_idx), 0) + 1 FROM refine_queue WHERE status = 'pending'")
            next_idx = cur.fetchone()[0]

            now = _now_iso()
            refining_cfg = json.dumps(item.get("refining_config", {}))
            cur.execute("""
                INSERT INTO refine_queue 
                (media_type, show_name, season, episode, title, raw_drive_path, target_refined_path, status, order_idx, refining_config, convert_pct, raw_size_mb, final_size_mb, chunk_status, error_msg, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, 0.0, ?, 0.0, '{}', '', ?, ?)
            """, (
                item.get("media_type", "other"),
                item.get("show_name", ""),
                int(item.get("season", 0)),
                int(item.get("episode", 0)),
                item.get("title", "Untitled Media"),
                item.get("raw_drive_path", ""),
                item.get("target_refined_path", ""),
                next_idx,
                refining_cfg,
                float(item.get("raw_size_mb", 0.0)),
                now,
                now
            ))
            _commit_and_sync(conn, force=True)
            return cur.lastrowid
        finally:
            conn.close()

def get_download_queue() -> List[Dict[str, Any]]:
    """Fetches all download queue items ordered logically: active first, then pending by order_idx, then finished."""
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM download_queue
                ORDER BY 
                    CASE 
                        WHEN status = 'downloading' THEN 0 
                        WHEN status = 'staging' THEN 0
                        WHEN status = 'pending' THEN 1 
                        ELSE 2 
                    END,
                    order_idx ASC, id ASC
            """)
            rows = cur.fetchall()
            result = []
            for r in rows:
                d = dict(r)
                try:
                    d["refining_config"] = json.loads(d.get("refining_config", "{}"))
                except Exception:
                    d["refining_config"] = {}
                result.append(d)
            return result
        finally:
            conn.close()

def get_refine_queue() -> List[Dict[str, Any]]:
    """Fetches all refine queue items ordered logically: active first, then pending by order_idx, then finished."""
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM refine_queue
                ORDER BY 
                    CASE 
                        WHEN status = 'refining' THEN 0 
                        WHEN status = 'pending' THEN 1 
                        ELSE 2 
                    END,
                    order_idx ASC, id ASC
            """)
            rows = cur.fetchall()
            result = []
            for r in rows:
                d = dict(r)
                try:
                    d["refining_config"] = json.loads(d.get("refining_config", "{}"))
                except Exception:
                    d["refining_config"] = {}
                try:
                    d["chunk_status"] = json.loads(d.get("chunk_status", "{}"))
                except Exception:
                    d["chunk_status"] = {}
                result.append(d)
            return result
        finally:
            conn.close()

def move_queue_item(table_name: str, item_id: int, direction: str) -> bool:
    """
    Moves a pending queue item 'up' or 'down' by cleanly swapping its rank
    among all pending items in the queue. 100% immune to ties and gaps.
    """
    if table_name not in ["download_queue", "refine_queue"]:
        return False
    if direction not in ["up", "down"]:
        return False

    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            # Fetch all pending items ordered by order_idx ASC, id ASC for this partition
            if table_name == "download_queue":
                cur.execute("SELECT is_low_speed FROM download_queue WHERE id = ?", (item_id,))
                row_t = cur.fetchone()
                is_low = int(row_t["is_low_speed"] or 0) if row_t else 0
                cur.execute("SELECT id, order_idx FROM download_queue WHERE status = 'pending' AND is_low_speed = ? ORDER BY order_idx ASC, id ASC", (is_low,))
            else:
                cur.execute(f"SELECT id, order_idx FROM {table_name} WHERE status = 'pending' ORDER BY order_idx ASC, id ASC")
            rows = [dict(r) for r in cur.fetchall()]
            if len(rows) < 2:
                return False

            item_indices = [i for i, r in enumerate(rows) if r["id"] == item_id]
            if not item_indices:
                return False

            idx = item_indices[0]
            if direction == "up" and idx > 0:
                target_idx = idx - 1
            elif direction == "down" and idx < len(rows) - 1:
                target_idx = idx + 1
            else:
                return False

            # Swap positions
            rows[idx], rows[target_idx] = rows[target_idx], rows[idx]

            now = _now_iso()
            # Reassign clean sequential order_indices 1, 2, 3...
            for rank, r in enumerate(rows, start=1):
                cur.execute(f"UPDATE {table_name} SET order_idx = ?, updated_at = ? WHERE id = ?", (rank, now, r["id"]))

            _commit_and_sync(conn, force=True)
            return True
        finally:
            conn.close()

def claim_next_download_job(is_low_speed: bool = False) -> Optional[Dict[str, Any]]:
    """
    Atomically claims the next pending download job in strict order (order_idx ASC, id ASC)
    for either high-speed (is_low_speed=False) or low-speed (is_low_speed=True) lane
    and transitions status to 'downloading'.
    """
    low_val = 1 if is_low_speed else 0
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM download_queue 
                WHERE status = 'pending' AND is_low_speed = ?
                ORDER BY order_idx ASC, id ASC LIMIT 1
            """, (low_val,))
            row = cur.fetchone()
            if not row:
                return None

            job_id = row["id"]
            now = _now_iso()
            cur.execute("""
                UPDATE download_queue 
                SET status = 'downloading', updated_at = ? 
                WHERE id = ? AND status = 'pending'
            """, (now, job_id))
            _commit_and_sync(conn, force=True)

            if cur.rowcount == 0:
                return None

            d = dict(row)
            d["status"] = "downloading"
            try:
                d["refining_config"] = json.loads(d.get("refining_config", "{}"))
            except Exception:
                d["refining_config"] = {}
            return d
        finally:
            conn.close()

def demote_to_low_speed_queue(item_id: int, reason: str = "Moved to Low-Speed Queue (< 100 KB/s for 120s)") -> bool:
    """Moves a download task from high-speed queue to low-speed trickle queue, placing it at the end of the pending low-speed queue."""
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COALESCE(MAX(order_idx), 0) + 1 FROM download_queue WHERE is_low_speed = 1 AND status = 'pending'")
            next_idx = cur.fetchone()[0]
            now = _now_iso()
            cur.execute("""
                UPDATE download_queue 
                SET is_low_speed = 1, status = 'pending', order_idx = ?, error_msg = ?, updated_at = ? 
                WHERE id = ?
            """, (next_idx, reason, now, item_id))
            _commit_and_sync(conn, force=True)
            return cur.rowcount > 0
        finally:
            conn.close()

def requeue_download_for_retry(item_id: int, max_retries: int = 2) -> Tuple[bool, int]:
    """
    If current retry_count < max_retries:
        increments retry_count, resets status to 'pending', moves to the end of the queue.
        Returns (True, new_retry_count).
    Else:
        marks as 'failed' with timeout reason.
        Returns (False, current_retry_count).
    """
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT retry_count, is_low_speed FROM download_queue WHERE id = ?", (item_id,))
            row = cur.fetchone()
            if not row:
                return False, 0
            curr_retries = int(row["retry_count"] or 0)
            is_low = int(row["is_low_speed"] or 0)
            now = _now_iso()

            if curr_retries < max_retries:
                new_retries = curr_retries + 1
                cur.execute("SELECT COALESCE(MAX(order_idx), 0) + 1 FROM download_queue WHERE is_low_speed = ? AND status = 'pending'", (is_low,))
                next_idx = cur.fetchone()[0]
                reason = f"Start timeout (90s): attempt {new_retries}/{max_retries + 1} - moved to end of queue"
                cur.execute("""
                    UPDATE download_queue 
                    SET retry_count = ?, status = 'pending', order_idx = ?, download_pct = 0.0, download_speed = '--', error_msg = ?, updated_at = ? 
                    WHERE id = ?
                """, (new_retries, next_idx, reason, now, item_id))
                _commit_and_sync(conn, force=True)
                return True, new_retries
            else:
                err_msg = f"Failed to start within 90s after {max_retries + 1} attempts (no active swarm/seeds)"
                cur.execute("""
                    UPDATE download_queue 
                    SET status = 'failed', error_msg = ?, updated_at = ? 
                    WHERE id = ?
                """, (err_msg, now, item_id))
                _commit_and_sync(conn, force=True)
                return False, curr_retries + 1
        finally:
            conn.close()

def toggle_download_queue_speed(item_id: int, to_low_speed: Optional[bool] = None) -> bool:
    """Manually switches a pending download job between high-speed queue and low-speed queue."""
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT is_low_speed, status FROM download_queue WHERE id = ?", (item_id,))
            row = cur.fetchone()
            if not row:
                return False
            curr = int(row["is_low_speed"] or 0)
            target = (1 if to_low_speed else 0) if to_low_speed is not None else (0 if curr == 1 else 1)

            cur.execute("SELECT COALESCE(MAX(order_idx), 0) + 1 FROM download_queue WHERE is_low_speed = ? AND status = 'pending'", (target,))
            next_idx = cur.fetchone()[0]
            now = _now_iso()
            cur.execute("""
                UPDATE download_queue 
                SET is_low_speed = ?, order_idx = ?, updated_at = ? 
                WHERE id = ?
            """, (target, next_idx, now, item_id))
            _commit_and_sync(conn, force=True)
            return cur.rowcount > 0
        finally:
            conn.close()

def claim_next_refine_job() -> Optional[Dict[str, Any]]:
    """
    Atomically claims the next pending refining job in strict order (order_idx ASC, id ASC)
    and transitions status to 'refining'.
    """
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM refine_queue 
                WHERE status = 'pending'
                ORDER BY order_idx ASC, id ASC LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                return None

            job_id = row["id"]
            now = _now_iso()
            cur.execute("""
                UPDATE refine_queue 
                SET status = 'refining', updated_at = ? 
                WHERE id = ? AND status = 'pending'
            """, (now, job_id))
            _commit_and_sync(conn, force=True)

            if cur.rowcount == 0:
                return None

            d = dict(row)
            d["status"] = "refining"
            try:
                d["refining_config"] = json.loads(d.get("refining_config", "{}"))
            except Exception:
                d["refining_config"] = {}
            try:
                d["chunk_status"] = json.loads(d.get("chunk_status", "{}"))
            except Exception:
                d["chunk_status"] = {}
            return d
        finally:
            conn.close()

def update_item_status(table_name: str, item_id: int, **kwargs):
    """Dynamically updates status, progress, speed, errors, or custom fields for a queue row."""
    if table_name not in ["download_queue", "refine_queue"]:
        return

    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            fields = []
            values = []
            for k, v in kwargs.items():
                if k in ["refining_config", "chunk_status"] and isinstance(v, (dict, list)):
                    v = json.dumps(v)
                fields.append(f"{k} = ?")
                values.append(v)

            fields.append("updated_at = ?")
            values.append(_now_iso())
            values.append(item_id)

            sql = f"UPDATE {table_name} SET {', '.join(fields)} WHERE id = ?"
            cur.execute(sql, tuple(values))
            is_high_frequency = any(k in kwargs for k in ["download_pct", "convert_pct", "download_speed"])
            _commit_and_sync(conn, force=not is_high_frequency)
        finally:
            conn.close()

def complete_refine_job(job_id: int, final_size_mb: float = 0.0):
    """Marks a refine job as completed with 100% progress."""
    update_item_status(
        "refine_queue",
        job_id,
        status="completed",
        convert_pct=100.0,
        final_size_mb=final_size_mb,
        error_msg=""
    )

def get_queue_item(table_name: str, item_id: int) -> Optional[Dict[str, Any]]:
    """Retrieves a single item from the specified queue table."""
    if table_name not in ["download_queue", "refine_queue"]:
        return None
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {table_name} WHERE id = ?", (item_id,))
            row = cur.fetchone()
            if not row:
                return None
            d = dict(row)
            try:
                d["refining_config"] = json.loads(d.get("refining_config", "{}"))
            except Exception:
                d["refining_config"] = {}
            if "chunk_status" in d:
                try:
                    d["chunk_status"] = json.loads(d.get("chunk_status", "{}"))
                except Exception:
                    d["chunk_status"] = {}
            return d
        finally:
            conn.close()

def delete_queue_item(table_name: str, item_id: int) -> bool:
    """Deletes an item from the specified queue table."""
    if table_name not in ["download_queue", "refine_queue"]:
        return False
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"DELETE FROM {table_name} WHERE id = ?", (item_id,))
            _commit_and_sync(conn, force=True)
            return cur.rowcount > 0
        finally:
            conn.close()

def cancel_queue_item(table_name: str, item_id: int, reason: str = "Cancelled by user") -> bool:
    """Cancels a queue item and preserves or records cancellation reason."""
    if table_name not in ["download_queue", "refine_queue"]:
        return False
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                f"UPDATE {table_name} SET status = 'cancelled', error_msg = CASE WHEN error_msg IS NULL OR error_msg = '' THEN ? ELSE error_msg END, updated_at = ? WHERE id = ?",
                (reason, _now_iso(), item_id)
            )
            _commit_and_sync(conn, force=True)
            return cur.rowcount > 0
        finally:
            conn.close()

def clear_completed(table_name: str) -> int:
    """Purges completed, failed, or cancelled jobs from the specified queue table."""
    if table_name not in ["download_queue", "refine_queue"]:
        return 0
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"DELETE FROM {table_name} WHERE status IN ('completed', 'failed', 'cancelled')")
            deleted = cur.rowcount
            _commit_and_sync(conn, force=True)
            return deleted
        finally:
            conn.close()
