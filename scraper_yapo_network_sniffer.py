from __future__ import annotations

import json
import time

from selenium import webdriver


TARGET_URL = (
    "https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/"
    "region-metropolitana-nunoa"
)
KEYWORDS = ("search", "list", "api", "mercadolibre")
RESOURCE_TYPES = {"XHR", "Fetch"}


def build_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--auto-open-devtools-for-tabs")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    return webdriver.Chrome(options=options)


def extract_network_events(logs):
    events = []

    for entry in logs:
        try:
            message = json.loads(entry.get("message", "{}")).get("message", {})
        except json.JSONDecodeError:
            continue

        if message.get("method") != "Network.responseReceived":
            continue

        params = message.get("params", {})
        response = params.get("response", {})
        resource_type = params.get("type")
        url = response.get("url")

        if resource_type not in RESOURCE_TYPES:
            continue

        if not url:
            continue

        events.append(
            {
                "url": url,
                "type": resource_type,
                "status": response.get("status"),
                "mimeType": response.get("mimeType"),
            }
        )

    return events


def is_relevant_url(url):
    normalized_url = url.lower()
    return any(keyword in normalized_url for keyword in KEYWORDS)


def main():
    driver = build_driver()

    try:
        driver.get(TARGET_URL)
        time.sleep(10)

        logs = driver.get_log("performance")
        events = extract_network_events(logs)
        seen_urls = set()

        for event in events:
            url = event["url"]

            if url in seen_urls:
                continue

            seen_urls.add(url)

            if is_relevant_url(url):
                print("POSSIBLE API:", url)
                print(
                    "  "
                    f"type={event.get('type')} "
                    f"status={event.get('status')} "
                    f"mime={event.get('mimeType')}"
                )

        print(f"Total XHR/fetch responses captured: {len(events)}")
        print(f"Relevant unique URLs: {len([url for url in seen_urls if is_relevant_url(url)])}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
