"""
Script final del pipeline que convierte el archivo JSON enriquecido a un reporte CSV.

Este script lee el JSON generado por WebsiteContactFinder.py, aplica un filtro
para procesar los registros (por defecto, solo aquellos con un email encontrado),
parsea la dirección en componentes separados (calle, unidad, ciudad) y escribe
los resultados en un archivo CSV limpio y listo para usar.
"""
import json
import csv
import re
import argparse
import sys

def parse_address_details_specific(full_address_str):
    """Parsea una dirección en componentes: calle, unidad, y ciudad."""
    if not full_address_str or not isinstance(full_address_str, str):
        return {"street": "", "unit": "", "city": ""}

    parts = [p.strip() for p in full_address_str.split(',')]
    street, unit, city = "", "", ""

    if len(parts) == 4 and (parts[1].lower().startswith("suite") or parts[1].lower().startswith("ste")):
        street, unit, city = parts[0], parts[1], parts[2]
    elif len(parts) >= 3:
        street, city = parts[0], parts[1]
    elif len(parts) == 2:
        street, city = parts[0], parts[1]
    elif len(parts) == 1:
        street = parts[0]
    
    return {"street": street.strip(), "unit": unit.strip(), "city": city.strip()}


def convertir_json_a_csv(input_json_file, output_csv_file, include_all=False):
    """Lee un JSON, lo procesa y lo convierte a CSV con opciones de filtrado."""
    print(f"Iniciando conversión: {input_json_file} -> {output_csv_file}")
    if include_all:
        print("Modo: Se incluirán todos los registros.")
    else:
        print("Modo: Se incluirán solo registros con email.")

    try:
        with open(input_json_file, 'r', encoding='utf-8') as f_json:
            business_data = json.load(f_json)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error Crítico al leer '{input_json_file}': {e}")
        return False

    # Columnas estandarizadas y ordenadas para el CSV
    csv_columns = ["Name", "Address", "unit number", "city", "email", "phone number"]
    records_for_csv = []

    for business in business_data:
        email = business.get("email_encontrado")

        if include_all or (email and email.strip()):
            address_details = parse_address_details_specific(business.get("direccion", ""))
            
            records_for_csv.append({
                "Name": business.get("nombre", ""),
                "Address": address_details["street"],
                "unit number": address_details["unit"],
                "city": address_details["city"],
                "email": email,
                "phone number": business.get("telefono", "")
                
            })

    if not records_for_csv:
        print("No se encontraron registros que cumplan con el criterio para escribir en el CSV.")
    
    try:
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as f_csv:
            writer = csv.DictWriter(f_csv, fieldnames=csv_columns)
            writer.writeheader()
            if records_for_csv:
                writer.writerows(records_for_csv)
        print(f"Archivo CSV '{output_csv_file}' generado exitosamente con {len(records_for_csv)} registros.")
        return True
    except Exception as e:
        print(f"Error al escribir el archivo CSV: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convierte un archivo JSON de negocios a CSV.")
    parser.add_argument("archivo_json_entrada", help="Archivo JSON de entrada.")
    parser.add_argument("archivo_csv_salida", help="Nombre del archivo CSV de salida.")
    parser.add_argument("--include-all", action="store_true", help="Incluye todos los registros en el CSV, no solo los que tienen email.")
    
    args = parser.parse_args()
    
    success = convertir_json_a_csv(args.archivo_json_entrada, args.archivo_csv_salida, args.include_all)
    
    if not success:
        sys.exit(1)