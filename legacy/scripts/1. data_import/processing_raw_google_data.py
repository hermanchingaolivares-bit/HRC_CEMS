# -*- coding: utf-8 -*-
"""
Created on Sat Dec 20 00:30:43 2025

@author: herma
"""
# processing_raw_google_data.py
# -*- coding: utf-8 -*-
import os
import pandas as pd
import logging
from utils import (convertir_fecha_estandar, dividir_y_agregar)
import pandas.api.types as ptypes
import time
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
raw_dir = os.path.join(project_root, 'data/raw/google_sheets')
processed_dir = os.path.join(project_root, 'data/processed')
os.makedirs(processed_dir, exist_ok=True)

def load_csv(name):
    path = os.path.join(raw_dir, name)
    try:
        df = pd.read_csv(path)
        logging.info(f"Cargado {name} con {len(df)} filas")
        return df
    except Exception as e:
        logging.error(f"No se pudo cargar {name}: {e}")
        return pd.DataFrame()

def save_csv(df, name):
    path = os.path.join(processed_dir, name)
    df.to_csv(path, index=False)
    logging.info(f"Guardado {name} con {len(df)} filas")

# --- PMP2025 ---
def process_pmp2025():
    df = load_csv("pmp2025_raw.csv")
    if df.empty:
        return

    df["CATEGORIA"] = df["CATEGORÍA"].map({"EC": "Equipo crítico", "ER": "Equipo relevante"}).fillna("NO_DEFINIDO")

    for col in ["SERIE", "NIC", "FRECUENCIA",
                "MANTENIMIENTO INTERNO O MANTENIMIENTO EXTERNO O CONTRATO",
                "PROVEEDOR", "SITUACIÓN (LPF)", "SERVICIO", "FP"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    cols = ["SERIE", "NIC", "CATEGORIA", "SERVICIO", "FRECUENCIA",
            "MANTENIMIENTO INTERNO O MANTENIMIENTO EXTERNO O CONTRATO", "PROVEEDOR", "FP", "SITUACIÓN (LPF)"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]
    df.rename(columns={"SITUACIÓN (LPF)": "ESTADO"}, inplace=True)

    df["id_unico"] = df["SERIE"]

    save_csv(df.reset_index(drop=True), "pmp2025_processed.csv")

# --- PMP IM>12 ---
def process_pmp_im_mayor_12():
    df = load_csv("pmp_im_raw.csv")
    if df.empty:
        return

    for col in ["SERIE", "NIC", "SERVICIO", "FRECUENCIA", "MANTENIMIENTO INTERNO O MANTENIMIENTO EXTERNO O CONTRATO", "PROVEEDOR", "ESTADO", "FP"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df["CATEGORIA"] = "Equipo con índice de mantenimiento mayor a 12"
    cols = ["SERIE", "NIC", "CATEGORIA", "SERVICIO", "FRECUENCIA",
            "MANTENIMIENTO INTERNO O MANTENIMIENTO EXTERNO O CONTRATO", "PROVEEDOR", "FP", "ESTADO"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]
    df["id_unico"] = df["SERIE"]

    save_csv(df.reset_index(drop=True), "pmp_im_processed.csv")

# --- AE ---
def process_ae():
    df = load_csv("ae_raw.csv")
    if df.empty:
        return
    df.dropna(subset=["SERIE"], inplace=True)

    df["SERIE_ORIGINAL"] = df["SERIE"].astype(str).str.strip().str.upper()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    df = df[df["SERIE"].notna() & (df["SERIE"] != "")]

    df.rename(columns={"N°": "DOCUMENTO", "OBSERVACIÓN": "REPORTE"}, inplace=True)
    df["TIPO"] = "ENTREGA"

    df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
    df.dropna(subset=["FECHA"], inplace=True)

    df["id_unico"] = df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"]

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    df = df[[c for c in cols if c in df.columns]]

    save_csv(df.reset_index(drop=True), "ae_processed.csv")

# --- AP ---
def process_ap():
    df = load_csv("ap_raw.csv")
    if df.empty:
        return
    df.dropna(subset=["SN EQUIPO EN PRESTAMO"], inplace=True)

    df["SERIE_ORIGINAL"] = df["SN EQUIPO EN PRESTAMO"].astype(str).str.strip().str.upper()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    df = df[df["SERIE"].notna() & (df["SERIE"] != "")]

    df.rename(columns={"N° AP ": "DOCUMENTO", "UNIDAD QUE ENTREGA": "REPORTE"}, inplace=True)
    df["TIPO"] = "PRESTAMO"

    df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
    df.dropna(subset=["FECHA"], inplace=True)

    df["id_unico"] = df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"]

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    df = df[[c for c in cols if c in df.columns]]

    save_csv(df.reset_index(drop=True), "ap_processed.csv")

# --- CS ---
def process_cs():
    df = load_csv("cs_raw.csv")
    if df.empty:
        return
    df.dropna(subset=["SERIE"], inplace=True)

    df["SERIE_ORIGINAL"] = df["SERIE"].astype(str).str.strip().str.upper()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    df = df[df["SERIE"].notna() & (df["SERIE"] != "")]

    df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
    df.dropna(subset=["FECHA"], inplace=True)

    df.rename(columns={" N°": "DOCUMENTO", "OBSERVACION": "REPORTE"}, inplace=True)
    df["TIPO"] = "SALIDA A SERVICIO TECNICO"

    df["id_unico"] = df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"]

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    df = df[[c for c in cols if c in df.columns]]

    save_csv(df.reset_index(drop=True), "cs_processed.csv")

# --- CATASTRO ---
def process_catastro():
    df = load_csv("catastro_raw.csv")
    if df.empty:
        return

    if "SERIE" in df.columns:
        df["SERIE"] = df["SERIE"].astype(str).str.strip().str.upper()

    if "AÑO DE ADQUISICIÓN" in df.columns:
        df["AÑO DE ADQUISICIÓN"] = pd.to_numeric(df["AÑO DE ADQUISICIÓN"], errors="coerce").astype('Int64')

    if "FECHA" in df.columns:
        df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)

    cols = ["SERIE", "NOMBRE EQUIPO", "N° INVENTARIO", "MODELO", "MARCA", "AÑO DE ADQUISICIÓN",
            "GESTIÓN AMBIENTAL", "FECHA", "RECINTO (SECTOR)", "RECINTO", "RESPONSABLE CATASTRO"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]

    if "NOMBRE EQUIPO" in df.columns:
        df.rename(columns={"NOMBRE EQUIPO": "EQUIPO"}, inplace=True)

    df["id_unico"] = df["SERIE"]

    save_csv(df.reset_index(drop=True), "catastro_processed.csv")

# --- OT2025 ---
def process_ot2025():
    df = load_csv("ot2025_raw.csv")
    if df.empty:
        return
    df.dropna(subset=["IDENTIFICACIÓN DEL EQUIPO"], inplace=True)

    df["SERIE_ORIGINAL"] = df["IDENTIFICACIÓN DEL EQUIPO"].astype(str).str.strip().str.upper()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    df = df[df["SERIE"].notna() & (df["SERIE"] != "")]

    df["FECHA"] = df["FECHA CIERRE"].apply(convertir_fecha_estandar) if "FECHA CIERRE" in df.columns else None
    df.dropna(subset=["FECHA"], inplace=True)

    df.rename(columns={
        "OT": "DOCUMENTO",
        "OBS CLÍNICA": "OBS_CLINICA",
        "OBS EEMM": "OBS_EEMM"
    }, inplace=True)

    df["REPORTE"] = "Problema: " + df["OBS_CLINICA"].fillna('') + " // Trabajo realizado: " + df["OBS_EEMM"].fillna('')
    df["TIPO"] = "ORDEN DE TRABAJO"

    df["id_unico"] = df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"]

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    df = df[[c for c in cols if c in df.columns]]

    save_csv(df.reset_index(drop=True), "ot2025_processed.csv")

# --- HDV IM ---
def process_hdv_im():
    df = load_csv("hdv_im_raw.csv")
    if df.empty:
        return

    for col in ["SERIE", "NIC"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    if "FECHA" in df.columns:
        df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
        df.dropna(subset=["FECHA"], inplace=True)

    if "ACTIVIDAD" in df.columns:
        df.rename(columns={"ACTIVIDAD": "REPORTE"}, inplace=True)
    else:
        df["REPORTE"] = ""

    if "TIPO" not in df.columns:
        df["TIPO"] = ""
    if "DOCUMENTO" not in df.columns:
        df["DOCUMENTO"] = ""

    df["id_unico"] = df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"]

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    df = df[[c for c in cols if c in df.columns]]

    save_csv(df.reset_index(drop=True), "hdv_im_processed.csv")

# --- AMFE ---


def process_amfe():
    df = load_csv("amfe_raw.csv")
    if df.empty:
        return
    
    if "Serie" in df.columns:
        df["SERIE"] = df["Serie"].astype(str).str.strip().str.upper()
    else:
        df["SERIE"] = ""
    
    if "Fecha" in df.columns:
        df["FECHA"] = df["Fecha"].apply(convertir_fecha_estandar)
        df["FECHA"] = pd.to_datetime(df["FECHA"], errors='coerce')
        df = df[df["FECHA"].notna()]
    else:
        df["FECHA"] = pd.NaT
    
    
    df.columns = [col.upper() for col in df.columns]
    cols = ["CRITICIDAD", "DOCUMENTO", "REPORTE", "FECHA", "SERIE", "ESTADO", "OBSERVACIONES"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]

    # Validar columna FECHA para crear id_unico
    if ptypes.is_datetime64_any_dtype(df["FECHA"]):
        df["id_unico"] = df["SERIE"] + "_" + df["FECHA"].dt.strftime('%Y-%m-%d')
    else:
        logging.error("La columna 'FECHA' no está en formato datetime. No se puede crear 'id_unico'.")
        df["id_unico"] = None
    
    save_csv(df.reset_index(drop=True), "amfe_processed.csv")

start_time = time.time()
if __name__ == "__main__":
    process_pmp2025()
    process_pmp_im_mayor_12()
    process_ae()
    process_ap()
    process_cs()
    process_catastro()
    process_ot2025()
    process_hdv_im()
    process_amfe()

    logging.info("Procesamiento completo de todos los CSV procesados.")
    
end_time = time.time()
print(f"Tiempo total en google_sheet_integration.py: {end_time - start_time:.2f} segundos")    