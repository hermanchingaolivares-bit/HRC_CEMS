# -*- coding: utf-8 -*-
"""
Created on Fri Dec 19 23:07:18 2025

@author: herma
"""
import os
import sys
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
import time

start_time = time.time()
# 1. Definir la ruta raíz del proyecto (3 niveles arriba del script)
proyecto_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..',''))
# 2. Cargar archivo .env desde la raíz del proyecto
dotenv_path = os.path.join(proyecto_raiz, '.env')
load_dotenv(dotenv_path)
# 3. Obtener ruta absoluta para las credenciales desde variable de entorno
ruta_credenciales = os.getenv('GOOGLE_CREDENTIALS_PATH')

print(f"Ruta absoluta raíz proyecto: {proyecto_raiz}")
print(f"Ruta .env usada: {dotenv_path}")
print(f"Ruta cargada de credenciales: {ruta_credenciales}")
print(f"¿Existe archivo de credenciales?: {os.path.exists(ruta_credenciales) if ruta_credenciales else 'No definido'}")

print(f"Ruta de credenciales cargada: {ruta_credenciales}")
print(f"¿Existe la ruta? {os.path.exists(ruta_credenciales) if ruta_credenciales else 'Ruta no definida'}")
if ruta_credenciales is None:
    logging.error("La variable de entorno 'GOOGLE_CREDENTIALS_PATH' no está definida. Verifica tu archivo .env.")
    sys.exit(1)

# Si la ruta no es absoluta, unirla con la raíz del proyecto
if not os.path.isabs(ruta_credenciales):
    ruta_credenciales = os.path.join(proyecto_raiz, ruta_credenciales)

ruta_credenciales = os.path.abspath(ruta_credenciales)

print(f"Ruta absoluta de credenciales usada: {ruta_credenciales}")
print(f"¿Existe el archivo de credenciales?: {os.path.exists(ruta_credenciales)}")

if not os.path.exists(ruta_credenciales):
    logging.error(f"El archivo de credenciales no existe en: {ruta_credenciales}")
    sys.exit(1)

# 4. Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 5. Función para conectar a Google Sheets usando credenciales de servicio
def google_connect_eemm(credentials_path=ruta_credenciales):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open("EEMM")
        logging.info("Conexión exitosa a Google Sheets.")
        return spreadsheet
    except Exception as e:
        logging.error(f"Error al conectar con Google Sheets: {e}")
        return None

# 6. Función base para leer hoja específica y convertir a DataFrame
def read_worksheet(spreadsheet, sheet_name, header_row=1):
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_values()
        if not data:
            logging.warning(f"Hoja '{sheet_name}' está vacía.")
            return pd.DataFrame()
        
        headers = data[header_row - 1]
        values = data[header_row:]
        df = pd.DataFrame(values, columns=headers)
        logging.info(f"Hoja '{sheet_name}' leída correctamente con {len(df)} registros.")
        return df
    except Exception as e:
        logging.error(f"Error al leer la hoja '{sheet_name}': {e}")
        return pd.DataFrame()

# 7. Funciones específicas para leer cada hoja del Google Sheet

def read_pmp2025(spreadsheet):
    df = read_worksheet(spreadsheet, "PMP2025", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

def read_pmp_im_mayor_12(spreadsheet):
    df = read_worksheet(spreadsheet, "PMP IM>12", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

def read_ae(spreadsheet):
    df = read_worksheet(spreadsheet, "AE", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

def read_ap(spreadsheet):
    df = read_worksheet(spreadsheet, "AP", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

def read_cs(spreadsheet):
    df = read_worksheet(spreadsheet, "CS", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

def read_catastro(spreadsheet):
    df = read_worksheet(spreadsheet, "CATASTRO", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

def read_ot2025(spreadsheet):
    df = read_worksheet(spreadsheet, "OT2025", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

def read_hdv_im(spreadsheet):
    df = read_worksheet(spreadsheet, "HDV IM≥12", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

def read_amfe(spreadsheet):
    df = read_worksheet(spreadsheet, "AMFE EQUIPOS", header_row=1)
    return df.reset_index(drop=True) if not df.empty else df

# 8. Código principal para ejecutar la carga y guardar CSVs con datos crudos
if __name__ == "__main__":
    spreadsheet = google_connect_eemm()
    if spreadsheet:
        # Leer todas las hojas y guardarlas en data/raw/google_sheets/
        output_dir = os.path.join(proyecto_raiz, 'data/raw/google_sheets')
        os.makedirs(output_dir, exist_ok=True)

        # Diccionario hoja -> función read y nombre archivo CSV
        hojas = {
            "PMP2025": (read_pmp2025, "pmp2025_raw.csv"),
            "PMP IM>12": (read_pmp_im_mayor_12, "pmp_im_raw.csv"),
            "AE": (read_ae, "ae_raw.csv"),
            "AP": (read_ap, "ap_raw.csv"),
            "CS": (read_cs, "cs_raw.csv"),
            "CATASTRO": (read_catastro, "catastro_raw.csv"),
            "OT2025": (read_ot2025, "ot2025_raw.csv"),
            "HDV IM≥12": (read_hdv_im, "hdv_im_raw.csv"),
            "AMFE EQUIPOS": (read_amfe, "amfe_raw.csv"),
        }

        for hoja_nombre, (funcion_lectura, archivo_csv) in hojas.items():
            df = funcion_lectura(spreadsheet)
            if not df.empty:
                ruta_csv = os.path.join(output_dir, archivo_csv)
                df.to_csv(ruta_csv, index=False)
                logging.info(f"Datos de '{hoja_nombre}' guardados en {ruta_csv}")
            else:
                logging.warning(f"No se guardaron datos para '{hoja_nombre}' porque el DataFrame está vacío.")
    else:
        logging.error("No se pudo conectar al spreadsheet. Verifica credenciales y conexión.")
        
end_time = time.time()
print(f"Tiempo total en google_sheet_integration.py: {end_time - start_time:.2f} segundos")