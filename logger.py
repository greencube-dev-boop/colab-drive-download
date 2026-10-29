import os
import sys
import time
import datetime
import threading
from typing import List, Dict, Any, Optional

# Thread-safe log storage & control
_log_lock = threading.RLock()
_debug_enabled: bool = True  # Enabled by default for easy debugging, can be toggled off
_recent_logs: List[Dict[str, Any]] = []
_MAX_LOG_HISTORY = 300
_log_file_path: Optional[str] = None

def set_debug(enabled: bool):
    """Enable or disable verbose debug logging across all modules."""
    global _debug_enabled
    with _log_lock:
        _debug_enabled = bool(enabled)
        os.environ["ENABLE_DEBUG_LOGS"] = "1" if _debug_enabled else "0"
        try:
            import config
            config.DEBUG_LOGS = _debug_enabled
        except Exception:
            pass

def is_debug() -> bool:
    """Returns True if verbose debug logging is enabled."""
    return _debug_enabled

def set_log_file(path: Optional[str]):
    """Optionally sets a persistent file path for logs (e.g. on Google Drive)."""
    global _log_file_path
    with _log_lock:
        _log_file_path = path

def _format_timestamp() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")

def _clear_console_line():
    """Wipes any pending \r ticker from the terminal line cleanly across OS platforms."""
    try:
        _safe_write("\r" + " " * 95 + "\r")
    except Exception:
        pass

def _safe_write(text: str):
    """Safely writes to stdout, automatically replacing unencodable Unicode chars on legacy consoles."""
    try:
        sys.stdout.write(text)
        sys.stdout.flush()
    except (UnicodeEncodeError, OSError):
        try:
            enc = getattr(sys.stdout, "encoding", None) or "utf-8"
            safe_text = text.encode(enc, errors="replace").decode(enc)
            sys.stdout.write(safe_text)
            sys.stdout.flush()
        except Exception:
            pass

def level_symbol(level: str) -> str:
    lvl = level.upper()
    if lvl == "INFO":
        return "ℹ️"
    elif lvl == "DEBUG":
        return "🔍"
    elif lvl in ("WARN", "WARNING"):
        return "⚠️"
    elif lvl == "ERROR":
        return "❌"
    elif lvl == "SUCCESS":
        return "✅"
    return "•"

def _emit_log(level: str, tag: str, message: str, force_print: bool = False):
    """
    Core logging emitter.
    Appends entry to in-memory buffer, writes to log file if set, and prints to console.
    """
    with _log_lock:
        if not force_print and level == "DEBUG" and not _debug_enabled:
            return

        ts = _format_timestamp()
        entry = {
            "timestamp": ts,
            "level": level,
            "tag": tag,
            "message": message,
            "time": time.time()
        }
        _recent_logs.append(entry)
        if len(_recent_logs) > _MAX_LOG_HISTORY:
            _recent_logs.pop(0)

        tag_str = f"[{tag}]" if tag else ""
        formatted = f"[{ts}] {level_symbol(level)} {tag_str} {message}"

        # Clear ongoing \r line before printing new log event
        _clear_console_line()
        _safe_write(formatted + "\n")

        if _log_file_path:
            try:
                os.makedirs(os.path.dirname(_log_file_path), exist_ok=True)
                with open(_log_file_path, "a", encoding="utf-8") as f:
                    f.write(formatted + "\n")
            except Exception:
                pass

def notify(tag: str, message: str):
    """High-priority notification milestone (always printed regardless of debug toggle)."""
    _emit_log("INFO", tag, message, force_print=True)

def success(tag: str, message: str):
    """Success notification milestone (always printed)."""
    _emit_log("SUCCESS", tag, message, force_print=True)

def debug(tag: str, message: str):
    """Detailed diagnostic log (printed only if debug is enabled)."""
    _emit_log("DEBUG", tag, message, force_print=False)

def warn(tag: str, message: str):
    """Warning event notification (always printed)."""
    _emit_log("WARN", tag, message, force_print=True)

def error(tag: str, message: str):
    """Error event notification (always printed)."""
    _emit_log("ERROR", tag, message, force_print=True)

def get_recent_logs(limit: int = 100) -> List[Dict[str, Any]]:
    """Returns recent log entries as structured dicts."""
    with _log_lock:
        return list(_recent_logs[-limit:])

def get_recent_logs_text(limit: int = 100) -> str:
    """Returns formatted multiline string of recent log entries."""
    with _log_lock:
        entries = _recent_logs[-limit:]
        if not entries:
            return "No log notifications recorded yet."
        lines = []
        for e in entries:
            tag_str = f"[{e['tag']}]" if e['tag'] else ""
            lines.append(f"[{e['timestamp']}] {level_symbol(e['level'])} {tag_str} {e['message']}")
        return "\n".join(lines)

def clear_logs():
    """Clears in-memory log history."""
    with _log_lock:
        _recent_logs.clear()
