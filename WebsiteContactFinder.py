"""
Scraper de sitios web para encontrar información de contacto.

Este script lee un archivo JSON que contiene una lista de negocios (generado
por YellowPagesCrawler.py), visita el sitio web de cada negocio e intenta
extraer un email de contacto.

Implementa una estrategia de dos pasos:
1. Un intento rápido usando la librería `requests`.
2. Si falla, un análisis más profundo usando Selenium para manejar
   contenido dinámico.

Como fallback, si no se encuentra un email, busca y extrae enlaces a la
página de Facebook del negocio.
"""
import json
import re
import time
import random
import requests
import argparse
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException

# --- Constantes y Configuración Global ---
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"

REQUESTS_HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
}

COMMON_CONTACT_PATHS = [
    "", "contact", "contacto", "contact-us", "contactus", "about", 
    "about-us", "nosotros", "impressum", "legal", "aviso-legal", "contactenos"
]

# Expresión regular para encontrar emails
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

# Expresión regular para encontrar URLs de perfiles/páginas de Facebook,
# excluyendo explícitamente URLs funcionales (sharer, plugins, etc.).
FACEBOOK_PROFILE_REGEX = re.compile(
    r'https?://(?:www\.)?facebook\.com/'
    r'(?!sharer|plugins|dialog|login\.php|share\.php|pages(?:/\w{0,15})?/badge|video\.php|watch|events|groups|search|help|legal|terms|privacy|policies|maps|notes|photo\.php|photos|media|marketplace|jobs|games|fundraisers|developers|careers|business|stories|live|messages|notifications|bookmarks|ads|gaming|page_insights|insights|activity_log|settings|recommendations|reviews|offers|services|shop|community|menu|events|videos|posts|questions|groups|albums|applications)'
    r'([\w.-]+(?:/[\w.-]+)*)/?'
)


def configurar_driver_selenium():
    """Configura e inicializa un driver de Selenium optimizado."""
    options = Options()
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument(f"user-agent={USER_AGENT}")
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    options.page_load_strategy = 'eager'
    
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        print("Driver de Selenium configurado.")
        return driver
    except Exception as e:
        print(f"Error al configurar el driver de Selenium: {e}")
        return None

def _extraer_primer_email_valido_de_texto(texto):
    """Busca en un bloque de texto y devuelve el primer email válido encontrado."""
    match = EMAIL_REGEX.search(texto)
    if match:
        email = match.group(0).lower()
        # Una segunda validación para asegurar que la regex no capturó algo inválido
        if EMAIL_REGEX.fullmatch(email):
            return email
    return None

def buscar_facebook_links_en_html(html_content, base_url):
    """Parsea contenido HTML y extrae todos los links de perfil/página de Facebook válidos."""
    links_encontrados = set()
    soup = BeautifulSoup(html_content, 'html.parser')
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        url_absoluta = urljoin(base_url, href)
        match = FACEBOOK_PROFILE_REGEX.match(url_absoluta)
        if match:
            path = urlparse(url_absoluta).path.strip('/')
            # Filtro simple para evitar URLs genéricas de Facebook
            if path and path not in ["facebook", "pg", "profile.php"]:
                links_encontrados.add(url_absoluta)
    return list(links_encontrados)

def buscar_primer_email_con_requests(url, session):
    """Intenta encontrar un email en una URL usando Requests (método rápido)."""
    print(f"  [Requests] Intentando en: {url}")
    try:
        respuesta = session.get(url, headers=REQUESTS_HEADERS, timeout=15, allow_redirects=True)
        respuesta.raise_for_status()
        
        # 1. Buscar en links 'mailto:'
        soup = BeautifulSoup(respuesta.content, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href_lower = a_tag.get('href', '').lower()
            if href_lower.startswith('mailto:'):
                email = href_lower.split('mailto:', 1)[1].split('?')[0].strip()
                if EMAIL_REGEX.fullmatch(email):
                    print(f"    -> Email (mailto) encontrado: {email}")
                    return email
        
        # 2. Buscar en el texto de la página
        email_en_texto = _extraer_primer_email_valido_de_texto(respuesta.text)
        if email_en_texto:
            print(f"    -> Email (texto) encontrado: {email_en_texto}")
            return email_en_texto
            
        return None
    except requests.exceptions.RequestException as e:
        print(f"    -> Error de Requests: {e}")
        return None

def buscar_datos_contacto_con_selenium(driver, base_url):
    """
    Intenta encontrar un email o link de Facebook usando Selenium para una
    búsqueda más profunda en varias páginas de un mismo sitio.
    """
    print(f"  [Selenium] Iniciando búsqueda profunda en el dominio de: {base_url}")
    email_final = None
    facebook_links_final = set()
    
    # Construir lista de URLs a visitar
    urls_a_visitar = set()
    if not urlparse(base_url).scheme: base_url = "http://" + base_url
    for path in COMMON_CONTACT_PATHS:
        urls_a_visitar.add(urljoin(base_url, path))

    for i, url in enumerate(list(urls_a_visitar)):
        # Si ya encontramos un email, no necesitamos seguir buscando
        if email_final:
            break
            
        print(f"    Visitando ({i+1}/{len(urls_a_visitar)}): {url}")
        try:
            driver.get(url)
            # Espera simple a que la página esté interactiva
            WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
            page_source = driver.page_source
            
            # Buscar email
            email_encontrado = _extraer_primer_email_valido_de_texto(page_source)
            if email_encontrado:
                print(f"      -> Email (texto) encontrado: {email_encontrado}")
                email_final = email_encontrado
                continue # Pasar a la siguiente URL del bucle principal
            
            # Si no hay email, buscar links de Facebook
            links_fb = buscar_facebook_links_en_html(page_source, url)
            if links_fb:
                facebook_links_final.update(links_fb)

        except Exception as e:
            print(f"      -> Error al procesar {url} con Selenium: {e}")
            continue

    return {'email': email_final, 'facebook_links': list(facebook_links_final)}

def is_valid_website(url):
    """Verifica si una URL es válida para ser procesada."""
    if not url or not isinstance(url, str):
        return False
    
    url_lower = url.lower().strip()
    if url_lower in ["no encontrado", "no encontrado (href vacío)"]:
        return False
        
    if not (url_lower.startswith('http://') or url_lower.startswith('https://')):
        return False
        
    return True

def procesar_negocios(input_json_path, driver):
    """Orquesta el proceso de búsqueda para una lista de negocios."""
    try:
        with open(input_json_path, 'r', encoding='utf-8') as f:
            businesses = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error Crítico: No se pudo cargar o decodificar {input_json_path}. Error: {e}")
        return []

    print(f"Se cargaron {len(businesses)} negocios de '{input_json_path}'.")
    final_results = []
    
    with requests.Session() as session:
        for i, business in enumerate(businesses):
            business_name = business.get("nombre", "N/A")
            website_url = business.get("website")
            print(f"\n[{i+1}/{len(businesses)}] Procesando: {business_name} | Sitio: {website_url}")

            final_email = None
            final_fb_links = []
            method_used = "Ninguno"

            if is_valid_website(website_url):
                # Intento 1: Requests (rápido)
                final_email = buscar_primer_email_con_requests(website_url, session)
                if final_email:
                    method_used = "Requests (Email)"
                else:
                    # Intento 2: Selenium (profundo), solo si el driver está disponible
                    if driver:
                        selenium_results = buscar_datos_contacto_con_selenium(driver, website_url)
                        if selenium_results.get('email'):
                            final_email = selenium_results['email']
                            method_used = "Selenium (Email)"
                        elif selenium_results.get('facebook_links'):
                            final_fb_links = selenium_results['facebook_links']
                            method_used = "Selenium (FB Links)"
                    else:
                         print("  -> Selenium no disponible para búsqueda profunda.")
            else:
                print("  -> Sitio web no válido o no proporcionado.")

            # Unir resultados al registro original
            result_entry = business.copy()
            result_entry["email_encontrado"] = final_email
            result_entry["facebook_links_encontrados"] = final_fb_links
            result_entry["metodo_extraccion"] = method_used
            final_results.append(result_entry)
            
            # Pausa para no saturar los servidores
            time.sleep(random.uniform(0.5, 1.5))

    return final_results

def main(input_file, output_file):
    """Función principal para orquestar todo el script."""
    print(f"Iniciando WebsiteContactFinder: {input_file} -> {output_file}")
    
    selenium_driver = configurar_driver_selenium()
    
    full_results = procesar_negocios(input_file, selenium_driver)
    
    if selenium_driver:
        selenium_driver.quit()
        print("\nDriver de Selenium cerrado.")

    if full_results:
        try:
            with open(output_file, 'w', encoding='utf-8') as f_out:
                json.dump(full_results, f_out, indent=4, ensure_ascii=False)
            
            # Resumen final
            emails_count = sum(1 for r in full_results if r.get("email_encontrado"))
            fb_count = sum(1 for r in full_results if not r.get("email_encontrado") and r.get("facebook_links_encontrados"))
            
            print(f"\n--- Proceso Completado ---")
            print(f"{len(full_results)} registros procesados y guardados en '{output_file}'")
            print(f"  - Emails encontrados: {emails_count}")
            print(f"  - Links de Facebook (sin email): {fb_count}")
        except Exception as e:
            print(f"Error Crítico al guardar los resultados en {output_file}: {e}")
    else:
        print("\nNo se procesaron datos o no se generaron resultados para guardar.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Busca emails y/o links de Facebook en sitios web listados en un archivo JSON.")
    parser.add_argument("archivo_json_entrada", help="Archivo JSON de entrada (salida del crawler)")
    parser.add_argument("archivo_json_salida", help="Archivo JSON de salida (enriquecido)")
    
    args = parser.parse_args()
    
    main(args.archivo_json_entrada, args.archivo_json_salida)













































