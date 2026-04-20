from __future__ import annotations

import random
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36"
    ),
]


def create_driver():
    options = Options()

    # stealth base
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # estabilidad
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=es-CL")

    # viewport real
    options.add_argument("window-size=1920,1080")

    # user agent rotatorio
    ua = random.choice(USER_AGENTS)
    options.add_argument(f"user-agent={ua}")

    driver = webdriver.Chrome(options=options)

    # esconder webdriver
    driver.execute_script(
        """
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        })
        """
    )

    print(f"[STEALTH V3] Driver inicializado | UA: {ua}")

    return driver


def human_scroll(driver):
    try:
        height = driver.execute_script("return document.body.scrollHeight")

        driver.execute_script(f"window.scrollTo(0, {height * 0.3});")
        time.sleep(random.uniform(0.5, 1.5))

        driver.execute_script(f"window.scrollTo(0, {height * 0.6});")
        time.sleep(random.uniform(0.5, 1.5))

        driver.execute_script(f"window.scrollTo(0, {height});")
        time.sleep(random.uniform(1.0, 2.0))

    except Exception as error:
        print(f"[SCROLL ERROR] {error}")


def load_page(driver, url):
    delay = random.uniform(3.0, 7.0)
    print(f"[PAGE DELAY V3] {delay:.2f}s")
    time.sleep(delay)

    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except Exception:
        print("[WARNING] body no detectado")

    time.sleep(random.uniform(1.0, 2.5))

    # scroll humano
    human_scroll(driver)

    # detección básica de bloqueo
    if "captcha" in driver.page_source.lower():
        print("[BLOCK DETECTED] CAPTCHA detectado")
        return False

    return True


def scrape_with_retry(driver, url):
    success = load_page(driver, url)

    if not success:
        print("[RETRY] reintentando página...")
        time.sleep(random.uniform(5.0, 10.0))
        success = load_page(driver, url)

    return success


def scrape_comuna_stealth(comuna, base_url, pages=4):
    driver = create_driver()

    try:
        for page in range(1, pages + 1):
            url = f"{base_url}?o={page}" if page > 1 else base_url

            print(f"[SCRAPING] {comuna} página {page}")

            ok = scrape_with_retry(driver, url)

            if not ok:
                print(f"[SKIP] página {page} bloqueada")
                continue

            # delay humano entre páginas
            time.sleep(random.uniform(2.0, 5.0))

    finally:
        driver.quit()


if __name__ == "__main__":
    base = (
        "https://www.yapo.cl/bienes-raices-venta-de-propiedades-apartamentos/"
        "region-metropolitana-nunoa"
    )

    scrape_comuna_stealth("nunoa", base, pages=4)
