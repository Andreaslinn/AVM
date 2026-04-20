from datetime import datetime
import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "usage_log.json"
TRACKING_FILE = BASE_DIR / "user_tracking.json"


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


def _load_tracking():
    if TRACKING_FILE.exists():
        try:
            data = json.loads(TRACKING_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _save_tracking(data):
    TRACKING_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def save_tracking(username, listing_id):
    data = _load_tracking()
    user_items = data.setdefault(username, [])
    listing_id = str(listing_id)

    if listing_id in {str(item) for item in user_items}:
        return False

    user_items.append(listing_id)
    _save_tracking(data)
    return True


def get_tracking(username):
    data = _load_tracking()
    return [str(item) for item in data.get(username, [])]


def remove_tracking(username, listing_id):
    data = _load_tracking()
    listing_id = str(listing_id)
    user_items = [str(item) for item in data.get(username, [])]

    if listing_id not in user_items:
        return False

    data[username] = [item for item in user_items if item != listing_id]
    _save_tracking(data)
    return True
