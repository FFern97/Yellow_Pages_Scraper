"""
Orquestador principal para el pipeline de scraping de Yellow Pages.

Este script gestiona la ejecución secuencial de tres sub-procesos:
1. YellowPagesCrawler.py: Realiza el crawling inicial y guarda los resultados en JSON.
2. WebsiteContactFinder.py: Visita cada sitio web para encontrar emails/links y guarda un nuevo JSON.
3. Convert_json_to_csv.py: Convierte el JSON final a un reporte en formato CSV.

El script utiliza argparse para recibir los parámetros de búsqueda desde la línea de comandos.
Los archivos de salida se guardan en una carpeta 'results/'.
"""
import subprocess
import sys
import os
import argparse

# --- Configuración ---
PYTHON_EXECUTABLE = sys.executable  
YP_CRAWLER_SCRIPT = "YellowPagesCrawler.py"
SCRAP_FROM_JSON_SCRIPT = "WebsiteContactFinder.py"
CONVERT_JSON_TO_CSV_SCRIPT = "Convert_json_to_csv.py"

def run_script(script_path, args=None):
    """Ejecuta un script de Python y maneja su salida y errores."""
    command = [PYTHON_EXECUTABLE, script_path]
    if args:
        command.extend(args)
    
    print(f"\n--- Ejecutando Script: {' '.join(command)} ---")
    
    try:
        process = subprocess.run(
            command, 
            check=True, 
            capture_output=True, 
            text=True, 
            encoding='utf-8', 
            errors='replace'
        )
        if process.stdout:
            print("Salida del script:")
            print(process.stdout)
        if process.stderr:
            print("Errores del script (stderr):", file=sys.stderr)
            print(process.stderr, file=sys.stderr)
        print(f"--- Script {script_path} completado exitosamente. ---")
        return True
    except subprocess.CalledProcessError as e:
        print(f"!!! Error al ejecutar el script: {script_path} !!!", file=sys.stderr)
        print(f"Código de retorno: {e.returncode}", file=sys.stderr)
        if e.stdout:
            print(f"Salida (stdout) del error:\n{e.stdout}", file=sys.stderr)
        if e.stderr:
            print(f"Salida (stderr) del error:\n{e.stderr}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print(f"!!! Error: Script no encontrado en {script_path} !!!", file=sys.stderr)
        return False
    except Exception as ex:
        print(f"!!! Error inesperado al ejecutar {script_path}: {ex} !!!", file=sys.stderr)
        return False

def main():
    """Función principal que define y ejecuta el pipeline."""
    parser = argparse.ArgumentParser(description="Pipeline de scraping para Yellow Pages.")
    parser.add_argument("search_term", type=str, help="Término de búsqueda (ej. 'Chiropractor')")
    parser.add_argument("location_term", type=str, help="Ubicación de la búsqueda (ej. 'Fairfield County, CT')")
    parser.add_argument("-p", "--pages", type=int, default=1, help="Número máximo de páginas a scrapear (por defecto: 1)")
    
    args = parser.parse_args()

    search_term = args.search_term
    location_term = args.location_term
    max_pages_yp = args.pages

    print("Iniciando pipeline de scraping y procesamiento...")
    print(f"Parámetros: Término='{search_term}', Ubicación='{location_term}', Páginas='{max_pages_yp}'")

    # --- Generación de Nombres de Archivo ---
    s_term_safe = search_term.replace(' ', '_').lower()
    l_term_safe = location_term.replace(' ', '_').replace(',', '').lower()
    
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True) # Crea la carpeta 'results' si no existe

    yp_crawler_output_json = os.path.join(output_dir, f"step1_yp_data_{s_term_safe}_in_{l_term_safe}.json")
    contact_finder_output_json = os.path.join(output_dir, f"step2_contacts_found_{s_term_safe}_in_{l_term_safe}.json")
    final_csv_output = os.path.join(output_dir, f"step3_report_{s_term_safe}_in_{l_term_safe}.csv")

    # --- Paso 1: Ejecutar YellowPagesCrawler.py ---
    args_yp_crawler = [search_term, location_term, str(max_pages_yp), yp_crawler_output_json]
    if not run_script(YP_CRAWLER_SCRIPT, args_yp_crawler) or not os.path.exists(yp_crawler_output_json):
        print("Falló la etapa de crawling. Abortando pipeline.", file=sys.stderr)
        sys.exit(1)
    print(f"Resultado de {YP_CRAWLER_SCRIPT} guardado en: {yp_crawler_output_json}")

    # --- Paso 2: Ejecutar WebsiteContactFinder.py ---
    args_contact_finder = [yp_crawler_output_json, contact_finder_output_json]
    if not run_script(SCRAP_FROM_JSON_SCRIPT, args_contact_finder) or not os.path.exists(contact_finder_output_json):
        print(f"Falló la etapa de búsqueda de contactos. Abortando pipeline.", file=sys.stderr)
        sys.exit(1)
    print(f"Resultado de {SCRAP_FROM_JSON_SCRIPT} guardado en: {contact_finder_output_json}")

    # --- Paso 3: Ejecutar Convert_json_to_csv.py ---
    args_csv_converter = [contact_finder_output_json, final_csv_output]
    if not run_script(CONVERT_JSON_TO_CSV_SCRIPT, args_csv_converter) or not os.path.exists(final_csv_output):
        print(f"Falló la etapa de conversión a CSV. Abortando pipeline.", file=sys.stderr)
        sys.exit(1)
    print(f"Resultado de {CONVERT_JSON_TO_CSV_SCRIPT} guardado en: {final_csv_output}")

    print("\n\n--- ¡Pipeline completado exitosamente! ---")
    print(f"El reporte CSV final es: {final_csv_output}")

if __name__ == "__main__":
    main()