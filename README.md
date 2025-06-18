# Scraper de Yellow Pages

Este proyecto es un scraper automatizado diseñado para extraer información de contacto de negocios listados en Yellow Pages. El pipeline consta de tres etapas: crawling, scraping de sitios individuales y exportación a CSV.

## Sobre el Proyecto

El objetivo principal es construir una base de datos de contactos a partir de una categoría de negocio y una ubicación específicas. El proceso está orquestado por un script principal que gestiona la ejecución de los componentes individuales, pasando los datos de una etapa a la siguiente a través de archivos JSON.

Una característica clave es su lógica de fallback: si no se encuentra un email en el sitio web de un negocio, el scraper intentará localizar un enlace a su página de Facebook.

### Herramientas

* **Python 3.x**
* **ScrapeOps:** Integrado como servicio de proxy para evitar bloqueos y gestionar el scraping a gran escala. Una clave de API es
requerida para el funcionamiento del crawler.
* **Selenium:** Para la automatización del navegador y el manejo de contenido dinámico.
* **Beautiful Soup 4:** Para el parseo de HTML en la búsqueda de contactos.
* **python-dotenv:** Para la gestión segura de la clave de API.
* **webdriver-manager:** Para la gestión automática del ChromeDriver.
* **Argparse (Librería Estándar):** Para la gestión de argumentos de línea de comandos de forma profesional.

## Cómo Empezar

Sigue estos pasos para configurar y ejecutar el proyecto en tu máquina local.

### Prerrequisitos

* Tener Python instalado (versión 3.8 o superior recomendada).
* Tener Google Chrome instalado.

### Instalación

1.  **Clona o descarga el proyecto.**

2.  **Crea y activa un entorno virtual:**
    ```sh
    python -m venv venv
    .\venv\Scripts\activate
    ```

3.  **Instala las dependencias:** El archivo `requirements.txt` que creamos hace esto muy fácil.
    ```sh
    pip install -r requirements.txt
    ```

4.  **Configura tu API Key:** Crea un archivo llamado `.env` en la raíz del proyecto y añade tu clave de API de ScrapeOps. **Este paso es obligatorio para que el crawler funcione**.
    ```
    API_KEY="TU_API_KEY_DE_SCRAPEOPS_AQUI"
    ```

## Uso

El script principal (`main_pipeline.py`) se ejecuta desde la terminal, requiriendo un término de búsqueda y una ubicación. Opcionalmente, se puede especificar el número de páginas a procesar.

**Sintaxis:**
```sh
python main_pipeline.py "termino_de_busqueda" "ubicacion" --pages <numero>