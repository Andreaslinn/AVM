import json
from pathlib import Path


USERS_FILE = Path("users.json")


def create_users():
    if USERS_FILE.exists():
        users = json.loads(USERS_FILE.read_text(encoding="utf-8"))
    else:
        users = {}

    users["Andy"] = "1234"

    for i in range(1, 11):
        users[f"tester{i}"] = "1234"

    USERS_FILE.write_text(json.dumps(users, indent=2), encoding="utf-8")

    print("Test users created successfully")


if __name__ == "__main__":
    create_users()
