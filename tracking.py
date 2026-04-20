from datetime import datetime
import json
from pathlib import Path


LOG_FILE = Path("usage_log.json")


def _load_logs():
    if LOG_FILE.exists():
        try:
            return json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
    return []


def _save_logs(data):
    LOG_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def log_event(user, event, metadata=None, skip_tracking=False):
    if skip_tracking:
        return

    data = _load_logs()

    entry = {
        "user": user,
        "event": event,
        "timestamp": datetime.now().isoformat(),
    }

    if metadata:
        entry["metadata"] = metadata

    data.append(entry)
    _save_logs(data)
