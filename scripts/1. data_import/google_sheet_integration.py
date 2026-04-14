
# -*- coding: utf-8 -*-
"""
GOOGLE SHEETS → CSV RAW (Módulo limpio y exportable)
@author: herma
"""
import os
import logging
import time
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

def setup_proyecto():
    """Configuración inicial (credenciales + paths)"""
    proyecto_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..'))
    dotenv_path = os.path.join(proyecto_raiz, '.env')
    load_dotenv(dotenv_path)
    
    ruta_credenciales = os.getenv('GOOGLE_CREDENTIALS_PATH')
    if not ruta_credenciales:
        raise ValueError("❌ GOOGLE_CREDENTIALS_PATH no definida en .env")
    
    ruta_credenciales = os.path.abspath(os.path.join(proyecto_raiz, ruta_credenciales))
    if not os.path.exists(ruta_credenciales):
        raise FileNotFoundError(f"❌ Credenciales no encontradas: {ruta_credenciales}")
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return proyecto_raiz, ruta_credenciales

def google_connect_eemm(credentials_path=None):
    """Conexión a Google Sheets 'EEMM'"""
    _, ruta_credenciales = setup_proyecto() if credentials_path is None else (None, credentials_path)
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(ruta_credenciales, scopes=scope)
        client = gspread.authorize(creds)
        logging.info("✅ Conexión Google Sheets OK")
        return client.open("EEMM")
    except Exception as e:
        logging.error(f"❌ Error conexión: {e}")
        return None

def read_worksheet(spreadsheet, sheet_name, header_row=1):
    """Worksheet → DataFrame (ANTI-PANDAS-BUG 100% ROBUSTO)"""
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_values()
        if not data or len(data) <= header_row:
            logging.warning(f"Hoja '{sheet_name}' vacía")
            return pd.DataFrame()
        
        # 🔥 FIX DEFINITIVO: Construcción MANUAL sin inferencia automática
        headers = data[header_row-1]
        rows = data[header_row:]
        
        # Crea DataFrame SIN dtype inference
        df_list = []
        for row in rows:
            row_dict = {headers[i]: row[i] if i < len(row) else '' for i in range(len(headers))}
            df_list.append(row_dict)
            
        df = pd.DataFrame(df_list)
        
        # Limpieza final
        df = df.fillna('')
        df = df.astype(str)
        
        logging.info(f"'{sheet_name}': {len(df)} registros")
        return df.reset_index(drop=True)
        
    except Exception as e:
        logging.error(f"Error '{sheet_name}': {e}")
        return pd.DataFrame()

# 🎯 FUNCIONES DE LECTURA (nombres consistentes)
def read_pmp(spreadsheet): return read_worksheet(spreadsheet, "PMP")
def read_pmp_im(spreadsheet): return read_worksheet(spreadsheet, "PMP IM>12")
def read_ae(spreadsheet): return read_worksheet(spreadsheet, "AE")
def read_ap(spreadsheet): return read_worksheet(spreadsheet, "AP")
def read_cs(spreadsheet): return read_worksheet(spreadsheet, "CS")
def read_catastro(spreadsheet): return read_worksheet(spreadsheet, "CATASTRO")
def read_ot(spreadsheet): return read_worksheet(spreadsheet, "OT26")  # ← Consistente
def read_hdv_im(spreadsheet): return read_worksheet(spreadsheet, "HDV IM≥12")
def read_amfe(spreadsheet): return read_worksheet(spreadsheet, "AMFE EQUIPOS")
def read_it(spreadsheet): return read_worksheet(spreadsheet, "IT")

# 🔥 FUNCIÓN PRINCIPAL (exportable)
def cargar_todos_los_csvs(proyecto_raiz=None):
    """
    GOOGLE → CSVs RAW (usa esta función!)
    
    Uso directo:
    >>> python google_sheet_integration.py
    
    Uso import:
    >>> from google_sheet_integration import cargar_todos_los_csvs
    >>> cargar_todos_los_csvs()
    """
    if proyecto_raiz is None:
        proyecto_raiz, _ = setup_proyecto()
    
    output_dir = os.path.join(proyecto_raiz, 'data/raw/google_sheets')
    os.makedirs(output_dir, exist_ok=True)
    
    print("🔄 Conectando Google Sheets...")
    spreadsheet = google_connect_eemm()
    if not spreadsheet:
        logging.error("❌ No se pudo conectar")
        return False
    
    hojas = {
        "PMP": (read_pmp, "pmp_raw.csv"),
        "PMP IM>12": (read_pmp_im, "pmp_im_raw.csv"),
        "AE": (read_ae, "ae_raw.csv"),
        "AP": (read_ap, "ap_raw.csv"),
        "CS": (read_cs, "cs_raw.csv"),
        "CATASTRO": (read_catastro, "catastro_raw.csv"),
        "OT": (read_ot, "ot_raw.csv"),
        "HDV IM≥12": (read_hdv_im, "hdv_im_raw.csv"),
        "AMFE EQUIPOS": (read_amfe, "amfe_raw.csv"),
        "IT": (read_it, "it_raw.csv"),  # ← ⭐ NUEVA
    }
    total_filas = 0
    for nombre_hoja, (funcion, csv_name) in hojas.items():
        df = funcion(spreadsheet)
        if not df.empty:
            path = os.path.join(output_dir, csv_name)
            df.to_csv(path, index=False)
            total_filas += len(df)
            logging.info(f"📥 {nombre_hoja} → {csv_name} ({len(df)} filas)")
        else:
            logging.warning(f"⚠️  {nombre_hoja}: vacío")
    
    logging.info(f"✅ TOTAL: {total_filas} filas → {output_dir}")
    return True

# 🎯 EJECUCIÓN DIRECTA (mantener compatibilidad)
if __name__ == "__main__":
    start_time = time.time()
    exito = cargar_todos_los_csvs()
    print(f"\n⏱️  Tiempo: {time.time() - start_time:.2f}s")
    print("✅ CSVs generados en data/raw/google_sheets/")