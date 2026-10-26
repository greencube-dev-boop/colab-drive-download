import os
import json
import sqlite3
import datetime
import threading
from typing import Dict, Any, List, Optional
import config

_db_lock = threading.Lock()

def _get_connection() -> sqlite3.Connection:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(config.QUEUE_DB, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
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
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """)

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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_rf_status ON refine_queue(status, order_idx);")
            conn.commit()
        finally:
            conn.close()

def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def enqueue_download(item: Dict[str, Any]) -> int:
    """Inserts a new item into download_queue with next available order_idx."""
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COALESCE(MAX(order_idx), 0) + 1 FROM download_queue WHERE status = 'pending'")
            next_idx = cur.fetchone()[0]

            now = _now_iso()
            refining_cfg = json.dumps(item.get("refining_config", {}))
            cur.execute("""
                INSERT INTO download_queue 
                (title, url, source_type, status, order_idx, download_pct, download_speed, raw_path, refining_config, error_msg, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, 0.0, '--', '', ?, '', ?, ?)
            """, (
                item.get("title", "Untitled Download"),
                item.get("url", ""),
                item.get("source_type", "direct"),
                next_idx,
                refining_cfg,
                now,
                now
            ))
            conn.commit()
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
            conn.commit()
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
    Moves a pending queue item 'up' or 'down' by swapping its order_idx
    with the adjacent pending queue item.
    """
    if table_name not in ["download_queue", "refine_queue"]:
        return False
    if direction not in ["up", "down"]:
        return False

    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            # 1. Get current item's order_idx
            cur.execute(f"SELECT id, order_idx, status FROM {table_name} WHERE id = ?", (item_id,))
            current = cur.fetchone()
            if not current or current["status"] != "pending":
                return False

            curr_order = current["order_idx"]

            # 2. Find neighbor
            if direction == "up":
                cur.execute(f"""
                    SELECT id, order_idx FROM {table_name}
                    WHERE status = 'pending' AND (order_idx < ? OR (order_idx = ? AND id < ?))
                    ORDER BY order_idx DESC, id DESC LIMIT 1
                """, (curr_order, curr_order, item_id))
            else:
                cur.execute(f"""
                    SELECT id, order_idx FROM {table_name}
                    WHERE status = 'pending' AND (order_idx > ? OR (order_idx = ? AND id > ?))
                    ORDER BY order_idx ASC, id ASC LIMIT 1
                """, (curr_order, curr_order, item_id))

            neighbor = cur.fetchone()
            if not neighbor:
                return False

            neighbor_id = neighbor["id"]
            neighbor_order = neighbor["order_idx"]

            # If order_indices are identical, separate them
            if curr_order == neighbor_order:
                if direction == "up":
                    neighbor_order += 1
                else:
                    curr_order += 1

            # 3. Swap order_idx
            cur.execute(f"UPDATE {table_name} SET order_idx = ?, updated_at = ? WHERE id = ?", (neighbor_order, _now_iso(), item_id))
            cur.execute(f"UPDATE {table_name} SET order_idx = ?, updated_at = ? WHERE id = ?", (curr_order, _now_iso(), neighbor_id))
            conn.commit()
            return True
        finally:
            conn.close()

def claim_next_refine_job() -> Optional[Dict[str, Any]]:
    """
    Atomically claims the next pending refining job and sets status to 'refining'.
    Returns the job dict or None.
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
            conn.commit()

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
            conn.commit()
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

def cancel_queue_item(table_name: str, item_id: int) -> bool:
    """Cancels or deletes a queue item."""
    if table_name not in ["download_queue", "refine_queue"]:
        return False
    with _db_lock:
        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute(f"UPDATE {table_name} SET status = 'cancelled', updated_at = ? WHERE id = ?", (_now_iso(), item_id))
            conn.commit()
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
            conn.commit()
            return deleted
        finally:
            conn.close()
