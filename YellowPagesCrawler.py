"""
Crawler de Yellow Pages que utiliza Selenium y ScrapeOps.

Este script navega por las páginas de resultados de Yellow Pages para una búsqueda
y ubicación dadas, extrae la información básica de cada negocio (nombre,
teléfono, dirección, sitio web) y guarda los datos en un archivo JSON.

Utiliza ScrapeOps para gestionar proxies y evitar bloqueos.
"""
import os
import json
import time
import random
import argparse
from urllib.parse import urlencode, urljoin, urlparse, unquote, parse_qs
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# --- Configuración y Carga de Secretos ---
load_dotenv()  
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    print("ADVERTENCIA: API_KEY de ScrapeOps no encontrada en .env. Las peticiones se harán directamente.")
else:
    print("API Key de ScrapeOps cargada desde .env.")

# --- Constantes ---
BASE_YP_URL = "https://www.yellowpages.com"

# Selectores CSS (centralizados para fácil mantenimiento)
LISTING_CARD_SELECTOR = "div.search-results div.result, div.v-card"
BUSINESS_NAME_SELECTOR = "a.business-name"
PHONE_SELECTOR_PRIMARY = "div.phones.phone.primary"
PHONE_SELECTOR_FALLBACK = "div.phone"
STREET_ADDRESS_SELECTOR = "div.street-address"
LOCALITY_SELECTOR = "div.locality"
ADDRESS_FALLBACK_SELECTOR = ".adr"
WEBSITE_LINK_SELECTOR = "a.track-visit-website"
NEXT_BUTTON_BASE_SELECTOR = "a.next.ajax-page"

# Opciones de Chrome
CHROME_OPTIONS = Options()
# CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_argument("--no-sandbox")
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")
CHROME_OPTIONS.add_argument("--disable-gpu")
CHROME_OPTIONS.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")


def get_scrapeops_url(target_url, location="us"):
    """Construye la URL del proxy de ScrapeOps para una URL dada."""
    if not API_KEY:
        return target_url
    
    payload = {
        "api_key": API_KEY,
        "url": target_url,
        "country": location,
        "residential": True,
        "render_js": True,
        "timeout": 180000
    }
    return "https://proxy.scrapeops.io/v1/?" + urlencode(payload)

def _safe_find_text(element, by, value, default="No encontrado"):
    """Función auxiliar para encontrar un sub-elemento y devolver su texto de forma segura."""
    try:
        return element.find_element(by, value).text.strip()
    except NoSuchElementException:
        return default

def _safe_get_attribute(element, by, value, attribute="href", default="No encontrado"):
    """Función auxiliar para encontrar un sub-elemento y devolver un atributo de forma segura."""
    try:
        found_element = element.find_element(by, value)
        attr_value = found_element.get_attribute(attribute)
        return attr_value.strip() if attr_value and attr_value.strip() else default
    except NoSuchElementException:
        return default

def extract_listing_data(listing):
    """Extrae toda la información de un único listado (tarjeta de negocio)."""
    name = _safe_find_text(listing, By.CSS_SELECTOR, BUSINESS_NAME_SELECTOR)
    
    phone = _safe_find_text(listing, By.CSS_SELECTOR, PHONE_SELECTOR_PRIMARY)
    if phone == "No encontrado":
        phone = _safe_find_text(listing, By.CSS_SELECTOR, PHONE_SELECTOR_FALLBACK)
        
    street = _safe_find_text(listing, By.CSS_SELECTOR, STREET_ADDRESS_SELECTOR)
    locality = _safe_find_text(listing, By.CSS_SELECTOR, LOCALITY_SELECTOR)
    if street != "No encontrado" and locality != "No encontrado":
        address = f"{street}, {locality}"
    else:
        address = _safe_find_text(listing, By.CSS_SELECTOR, ADDRESS_FALLBACK_SELECTOR).replace('\n', ', ')
        
    website = _safe_get_attribute(listing, By.CSS_SELECTOR, WEBSITE_LINK_SELECTOR)

    return {"nombre": name, "telefono": phone, "direccion": address, "website": website}

def ejecutar_crawler_yp(search_term, location_term, max_pages, output_file):
    """
    Función principal que ejecuta el crawler para Yellow Pages.
    """
    print(f"Iniciando crawler para '{search_term}' en '{location_term}'...")
    initial_target_url = f"{BASE_YP_URL}/search?search_terms={search_term.replace(' ', '+')}&geo_location_terms={location_term.replace(' ', '+')}"
    
    driver = None
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=CHROME_OPTIONS)
        print("Driver de Chrome inicializado.")
    except Exception as e:
        print(f"Error fatal inicializando el driver de Chrome: {e}")
        return

    extracted_data = []
    processed_business_fingerprints = set() # Set para controlar duplicados
    current_page = 1
    next_page_url = initial_target_url

    try:
        while current_page <= max_pages and next_page_url:
            print(f"\n--- Procesando Página {current_page}/{max_pages} ---")
            
            scrapeops_url = get_scrapeops_url(next_page_url)
            print(f"Navegando a: {next_page_url} (vía ScrapeOps)")
            driver.get(scrapeops_url)

            try:
                WebDriverWait(driver, 45).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, LISTING_CARD_SELECTOR)))
                print("Listados detectados en la página.")
            except TimeoutException:
                print(f"Timeout esperando los listados en la página {current_page}. Terminando paginación.")
                break
            
            listings = driver.find_elements(By.CSS_SELECTOR, LISTING_CARD_SELECTOR)
            print(f"Se encontraron {len(listings)} elementos de listado (antes de filtrar duplicados).")

            for i, listing in enumerate(listings):
                listing_data = extract_listing_data(listing)
                
                # LÓGICA ANTI-DUPLICADOS
                fingerprint = f'{listing_data["nombre"]}-{listing_data["telefono"]}-{listing_data["direccion"]}'
                
                if fingerprint not in processed_business_fingerprints:
                    print(f"  Procesando listado único: {listing_data['nombre']}")
                    listing_data["pagina_origen"] = current_page
                    extracted_data.append(listing_data)
                    processed_business_fingerprints.add(fingerprint)
                else:
                    print(f"  Omitiendo listado duplicado: {listing_data['nombre']}")

            # Lógica de paginación
            try:
                selector_next = f'{NEXT_BUTTON_BASE_SELECTOR}[data-page="{current_page + 1}"]'
                next_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector_next)))
                raw_href = next_button.get_attribute('href')
                if raw_href:
                    if "proxy.scrapeops.io" in raw_href:
                        parsed_href = urlparse(raw_href)
                        query_params = parse_qs(parsed_href.query)
                        if 'url' in query_params:
                            next_page_url = unquote(query_params['url'][0])
                        else: next_page_url = None
                    else:
                        next_page_url = urljoin(BASE_YP_URL, raw_href)
                    print(f"URL de la siguiente página encontrada: {next_page_url}")
                else:
                    next_page_url = None
            except TimeoutException:
                print("No se encontró el botón 'Siguiente'. Asumiendo fin de los resultados.")
                next_page_url = None
            
            current_page += 1
            if next_page_url:
                time.sleep(random.uniform(2, 5))

    except Exception as e:
        print(f"Ocurrió un error general durante el scrapeo: {e}")
    finally:
        if driver:
            driver.quit()
            print("Driver cerrado.")

    if extracted_data:
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(extracted_data, f, indent=4, ensure_ascii=False)
            print(f"\nProceso completado. {len(extracted_data)} registros ÚNICOS guardados en {output_file}")
        except Exception as e_save:
            print(f"Error al guardar los resultados en {output_file}: {e_save}")
    else:
        print("\nNo se extrajeron datos.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper para Yellow Pages que extrae información de negocios.")
    parser.add_argument("search_term", help="Término de búsqueda (ej. Chiropractor)")
    parser.add_argument("location_term", help="Ubicación (ej. Fairfield County, CT)")
    parser.add_argument("max_pages", type=int, help="Número máximo de páginas a scrapear")
    parser.add_argument("output_file", help="Nombre del archivo JSON de salida")
    
    args = parser.parse_args()
    
    ejecutar_crawler_yp(args.search_term, args.location_term, args.max_pages, args.output_file)