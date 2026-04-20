from __future__ import annotations

import random
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from scraper_portalinmobiliario import parse_search_results as parse_portal_search_results


DEFAULT_WAIT_SECONDS = 20
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
LISTING_SELECTORS = [
    "li.ui-search-layout__item",
    "div.ui-search-result",
    "div.poly-card",
    "article",
    "a[href*='/MLC-']",
    "a[href*='portalinmobiliario.com']",
]


def iniciar_driver():
    options = Options()
    options.add_argument(f"--user-agent={USER_AGENT}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-infobars")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # headless=False para poder ver el navegador mientras se prueba el scraper.
    driver = webdriver.Chrome(service=Service(), options=options)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": (
                "Object.defineProperty(navigator, 'webdriver', {"
                "get: () => undefined"
                "});"
            )
        },
    )
    return driver


def fetch_with_selenium(url: str) -> str:
    driver = iniciar_driver()

    try:
        driver.get(url)

        wait = WebDriverWait(driver, DEFAULT_WAIT_SECONDS)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # Pausa realista para permitir carga dinámica y evitar comportamiento robótico.
        time.sleep(random.uniform(5, 8))

        scroll_like_user(driver)
        wait_for_listing_elements(driver)

        return driver.page_source
    finally:
        driver.quit()


def parse_search_results(html: str) -> list[dict]:
    # Instanciamos BeautifulSoup aquí para validar que el HTML sea parseable,
    # pero reutilizamos la lógica defensiva del scraper base.
    BeautifulSoup(html, "html.parser")
    listings = parse_portal_search_results(html)

    if not listings:
        print_debug_html(html)

    return listings


def scroll_like_user(driver) -> None:
    for scroll_y in (500, 1200, 2200):
        driver.execute_script("window.scrollTo(0, arguments[0]);", scroll_y)
        time.sleep(random.uniform(1, 2))

    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(random.uniform(1, 2))


def wait_for_listing_elements(driver) -> None:
    wait = WebDriverWait(driver, DEFAULT_WAIT_SECONDS)

    try:
        wait.until(
            lambda active_driver: any(
                active_driver.find_elements(By.CSS_SELECTOR, selector)
                for selector in LISTING_SELECTORS
            )
        )
    except TimeoutException:
        print("No se detectaron elementos de listings antes del timeout.")
        print("Puede que el sitio haya mostrado login, captcha o contenido bloqueado.")


def print_debug_html(html: str) -> None:
    snippet = html[:2000].replace("\n", " ")
    print("No se encontraron listings parseables.")
    print("Primeros 2000 caracteres del HTML para debug:")
    print(snippet)


if __name__ == "__main__":
    example_url = "https://listado.mercadolibre.cl/departamentos-en-venta-nunoa"
    html = fetch_with_selenium(example_url)
    listings = parse_search_results(html)

    print(f"Listings encontrados: {len(listings)}")
    for listing in listings[:5]:
        print(listing)
