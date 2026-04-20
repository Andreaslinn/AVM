from __future__ import annotations

import json
import os
import subprocess
import sys
from email.message import EmailMessage
from pathlib import Path
import smtplib

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    load_dotenv()

    health = run_scraper_health()
    alerts = get_alerts(health)

    if not alerts:
        print("OK")
        return 0

    email_body = build_email_body(health, alerts)
    send_alert_email(email_body)
    print(email_body)
    return 1


def run_scraper_health() -> dict:
    result = subprocess.run(
        [sys.executable, "scraper_health.py"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    stdout = result.stdout.strip()

    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        return {
            "status": "FAILED",
            "active_listings": 0,
            "previous_active_listings": 0,
            "errors": [
                f"No se pudo parsear JSON desde scraper_health.py: {exc}",
                f"returncode={result.returncode}",
                f"stdout={stdout[-1000:]}",
                f"stderr={result.stderr.strip()[-1000:]}",
            ],
        }


def get_alerts(health: dict) -> list[str]:
    alerts = []
    status = health.get("status")
    active_listings = health.get("active_listings") or 0
    previous_active_listings = health.get("previous_active_listings") or 0

    if status == "FAILED":
        alerts.append("status == FAILED")

    if (
        previous_active_listings > 0
        and active_listings < previous_active_listings * 0.8
    ):
        alerts.append("active_listings cayó más de 20%")

    return alerts


def build_email_body(health: dict, alerts: list[str]) -> str:
    active_listings = health.get("active_listings") or 0
    previous_active_listings = health.get("previous_active_listings") or 0
    variation_pct = calculate_variation_pct(active_listings, previous_active_listings)
    errors = health.get("errors") or []
    last_errors = errors[-5:]

    lines = [
        "Alerta de salud del scraper",
        "",
        "Problemas detectados:",
        *[f"- {alert}" for alert in alerts],
        "",
        f"Estado: {health.get('status')}",
        f"active_listings: {active_listings}",
        f"previous_active_listings: {previous_active_listings}",
        f"variación %: {format_variation(variation_pct)}",
        "",
        "Últimos errores:",
    ]

    if last_errors:
        lines.extend(f"- {error}" for error in last_errors)
    else:
        lines.append("- Sin errores reportados")

    return "\n".join(lines)


def calculate_variation_pct(active_listings: int, previous_active_listings: int):
    if previous_active_listings <= 0:
        return None

    return (active_listings - previous_active_listings) / previous_active_listings * 100


def format_variation(value) -> str:
    if value is None:
        return "Sin dato"

    return f"{value:.1f}%"


def send_alert_email(body: str) -> None:
    sender = os.environ.get("ALERT_EMAIL_FROM")
    recipient = os.environ.get("ALERT_EMAIL_TO")
    password = os.environ.get("ALERT_EMAIL_PASSWORD")
    smtp_host = os.environ.get("ALERT_SMTP_HOST")

    missing = [
        name
        for name, value in {
            "ALERT_EMAIL_FROM": sender,
            "ALERT_EMAIL_TO": recipient,
            "ALERT_EMAIL_PASSWORD": password,
            "ALERT_SMTP_HOST": smtp_host,
        }.items()
        if not value
    ]

    if missing:
        print(f"No se envió email: faltan variables {', '.join(missing)}")
        return

    message = EmailMessage()
    message["Subject"] = "Alerta scraper tasador_simple"
    message["From"] = sender
    message["To"] = recipient
    message.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, 587) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(message)
    except Exception as exc:
        print(f"Error enviando email de alerta: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
