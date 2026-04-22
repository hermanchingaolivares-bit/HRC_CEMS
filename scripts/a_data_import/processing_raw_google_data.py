# -*- coding: utf-8 -*-
"""
PROCESSING RAW GOOGLE DATA → PROCESSED CSVs
@author: herma
@date: 2025-12-20
"""
import os
import pandas as pd
import logging
from utils import (convertir_fecha_estandar, dividir_y_agregar)
import time

# 🔧 Configuración logging estandarizada
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
raw_dir = os.path.join(project_root, 'data/raw/google_sheets')
processed_dir = os.path.join(project_root, 'data/processed')
os.makedirs(processed_dir, exist_ok=True)

def load_csv(name):
    """Carga CSV raw con manejo robusto de errores"""
    path = os.path.join(raw_dir, name)
    if not os.path.exists(path):
        logging.warning(f"❌ {name} no encontrado")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, dtype=str, low_memory=False)
        logging.info(f"✅ {name}: {len(df):,} filas, {len(df.columns)} columnas")
        return df
    except Exception as e:
        logging.error(f"❌ Error cargando {name}: {e}")
        return pd.DataFrame()

def save_csv(df, name):
    """Guarda CSV procesado"""
    path = os.path.join(processed_dir, name)
    df.to_csv(path, index=False, encoding='utf-8')
    logging.info(f"💾 {name}: {len(df):,} filas guardadas")

# =============================================================================
# PROCESADORES INDIVIDUALES
# =============================================================================

def process_pmp():
    df = load_csv("pmp_raw.csv")
    if df.empty: return

    df["CATEGORIA"] = df["CATEGORÍA"].map({
        "EC": "Equipo crítico", 
        "ER": "Equipo relevante"
    }).fillna("NO_DEFINIDO")

    cols_to_clean = ["PMP","SEMESTRE","SERIE", "NIC", "FRECUENCIA",
                    "MANTENIMIENTO INTERNO O MANTENIMIENTO EXTERNO O CONTRATO",
                    "PROVEEDOR", "SITUACIÓN (LPF)", "SERVICIO", "FP"]
    
    for col in cols_to_clean:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    cols = ["PMP","SEMESTRE","SERIE", "NIC", "CATEGORIA", "SERVICIO", "FRECUENCIA",
            "MANTENIMIENTO INTERNO O MANTENIMIENTO EXTERNO O CONTRATO", "PROVEEDOR", "FP", "SITUACIÓN (LPF)"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].rename(columns={"SITUACIÓN (LPF)": "ESTADO"})
    df["id_unico"] = df["SERIE"]

    save_csv(df.reset_index(drop=True), "pmp2025_processed.csv")

def process_pmp_im_mayor_12():
    df = load_csv("pmp_im_raw.csv")
    if df.empty: return

    cols_to_clean = ["PMP","SERIE", "NIC", "SERVICIO", "FRECUENCIA", 
                    "MANTENIMIENTO INTERNO O MANTENIMIENTO EXTERNO O CONTRATO", "PROVEEDOR", "ESTADO", "FP"]
    
    for col in cols_to_clean:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df["CATEGORIA"] = "Equipo con índice de mantenimiento mayor a 12"
    cols = ["PMP","SERIE", "NIC", "CATEGORIA", "SERVICIO", "FRECUENCIA",
            "MANTENIMIENTO INTERNO O MANTENIMIENTO EXTERNO O CONTRATO", "PROVEEDOR", "FP", "ESTADO"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]
    df["id_unico"] = df["SERIE"]

    save_csv(df.reset_index(drop=True), "pmp_im_processed.csv")

def process_ae():
    df = load_csv("ae_raw.csv")
    if df.empty: return
    
    df.dropna(subset=["SERIE"], inplace=True)
    df["SERIE_ORIGINAL"] = df["SERIE"].astype(str).str.strip().str.upper()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    df = df[df["SERIE"].notna() & (df["SERIE"] != "")]

    df = df.rename(columns={"N°": "DOCUMENTO", "OBSERVACIÓN": "REPORTE"})
    df["TIPO"] = "ENTREGA"
    df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
    df.dropna(subset=["FECHA"], inplace=True)

    df["id_unico"] = (df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + 
                     df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"])

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    save_csv(df[cols].reset_index(drop=True), "ae_processed.csv")

def process_ap():
    df = load_csv("ap_raw.csv")
    if df.empty: return
    
    df.dropna(subset=["SN EQUIPO EN PRESTAMO"], inplace=True)
    df["SERIE_ORIGINAL"] = df["SN EQUIPO EN PRESTAMO"].astype(str).str.strip().str.upper()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    df = df[df["SERIE"].notna() & (df["SERIE"] != "")]

    df = df.rename(columns={"N° AP ": "DOCUMENTO", "UNIDAD QUE ENTREGA": "REPORTE"})
    df["TIPO"] = "PRESTAMO"
    df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
    df.dropna(subset=["FECHA"], inplace=True)

    df["id_unico"] = (df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + 
                     df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"])

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    save_csv(df[cols].reset_index(drop=True), "ap_processed.csv")

def process_cs():
    df = load_csv("cs_raw.csv")
    if df.empty: return
    
    df.dropna(subset=["SERIE"], inplace=True)
    df["SERIE_ORIGINAL"] = df["SERIE"].astype(str).str.strip().str.upper()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    df = df[df["SERIE"].notna() & (df["SERIE"] != "")]

    df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
    df.dropna(subset=["FECHA"], inplace=True)
    df = df.rename(columns={" N°": "DOCUMENTO", "OBSERVACION": "REPORTE"})
    df["TIPO"] = "SALIDA A SERVICIO TECNICO"

    df["id_unico"] = (df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + 
                     df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"])

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    save_csv(df[cols].reset_index(drop=True), "cs_processed.csv")

def process_catastro():
    df = load_csv("catastro_raw.csv")
    if df.empty: return

    if "SERIE" in df.columns:
        df["SERIE"] = df["SERIE"].astype(str).str.strip().str.upper()
    if "AÑO DE ADQUISICIÓN" in df.columns:
        df["AÑO DE ADQUISICIÓN"] = pd.to_numeric(df["AÑO DE ADQUISICIÓN"], errors="coerce").astype('Int64')
    if "FECHA" in df.columns:
        df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)

    cols = ["SERIE", "NOMBRE EQUIPO", "N° INVENTARIO", "MODELO", "MARCA", "AÑO DE ADQUISICIÓN",
            "GESTIÓN AMBIENTAL", "FECHA", "RECINTO (SECTOR)", "RECINTO", "RESPONSABLE CATASTRO"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].rename(columns={"NOMBRE EQUIPO": "EQUIPO"})
    df["id_unico"] = df["SERIE"]

    save_csv(df.reset_index(drop=True), "catastro_processed.csv")

def process_ot():
    df = load_csv("ot_raw.csv")
    if df.empty: return
    
    df.dropna(subset=["IDENTIFICACIÓN DEL EQUIPO"], inplace=True)
    df["SERIE_ORIGINAL"] = df["IDENTIFICACIÓN DEL EQUIPO"].astype(str).str.strip().str.upper()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    df = df[df["SERIE"].notna() & (df["SERIE"] != "")]

    fecha_col = "FECHA CIERRE" if "FECHA CIERRE" in df.columns else None
    if fecha_col:
        df["FECHA"] = df[fecha_col].apply(convertir_fecha_estandar)
        df.dropna(subset=["FECHA"], inplace=True)

    df = df.rename(columns={"OT": "DOCUMENTO", "OBS CLÍNICA": "OBS_CLINICA", "OBS EEMM": "OBS_EEMM"})
    df["REPORTE"] = ("Problema: " + df["OBS_CLINICA"].fillna('') + 
                    " // Trabajo realizado: " + df["OBS_EEMM"].fillna(''))
    df["TIPO"] = "ORDEN DE TRABAJO"

    df["id_unico"] = (df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + 
                     df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"])

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    save_csv(df[cols].reset_index(drop=True), "ot2025_processed.csv")

def process_hdv_im():
    df = load_csv("hdv_im_raw.csv")
    if df.empty: return

    for col in ["SERIE", "NIC"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    if "FECHA" in df.columns:
        df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
        df.dropna(subset=["FECHA"], inplace=True)

    df["REPORTE"] = df.get("ACTIVIDAD", "").fillna("")
    df["TIPO"] = df.get("TIPO", "")
    df["DOCUMENTO"] = df.get("DOCUMENTO", "")

    df["id_unico"] = (df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"] + "_" + 
                     df["DOCUMENTO"].astype(str).str.strip().str.upper() + "_" + df["TIPO"])

    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    save_csv(df[cols].reset_index(drop=True), "hdv_im_processed.csv")

def process_amfe():
    df = load_csv("amfe_raw.csv")
    if df.empty: return
    
    if "Serie" in df.columns:
        df["SERIE"] = df["Serie"].astype(str).str.strip().str.upper()
        df.drop("Serie", axis=1, inplace=True)
    
    if "Fecha" in df.columns:
        df["FECHA"] = df["Fecha"].apply(convertir_fecha_estandar)
        df.dropna(subset=["FECHA"], inplace=True)
        df.drop("Fecha", axis=1, inplace=True)
    
    df["id_unico"] = df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + df["SERIE"]
    df.columns = [col.upper() for col in df.columns]
    
    cols = ["FECHA","REPORTE","CRITICIDAD", "DOCUMENTO", "ESTADO", "SERIE", "OBSERVACIONES", "id_unico"]
    cols = [c for c in cols if c in df.columns]
    save_csv(df[cols].reset_index(drop=True), "amfe_processed.csv")

def process_it():
    """🔥 IT: Extrae series de 'Informe' texto libre"""
    df = load_csv("it_raw.csv")
    if df.empty:
        logging.warning("❌ it_raw.csv vacío")
        return
    
    
    # 📝 Extrae series del texto libre "Informe"
    df["SERIE_ORIGINAL"] = df["Informe"].astype(str).str.strip()
    df["SERIES"] = df["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    
    # 💥 Explode: 1 fila → N filas (1 por serie)
    df = df.explode("SERIES").reset_index(drop=True)
    df["SERIE"] = df["SERIES"].astype(str).str.strip().str.upper()
    
    # 🧹 Filtra series válidas
    df = df[df["SERIE"].notna() & 
            (df["SERIE"] != "") & 
            (df["SERIE"] != "NAN") & 
            (df["SERIE"] != "NONE")]
    
    # 📅 Procesa FECHA
    df["FECHA"] = df["FECHA"].apply(convertir_fecha_estandar)
    df.dropna(subset=["FECHA"], inplace=True)
    
    # 🎯 Columnas EXACTAS solicitadas
    df["TIPO"] = "INFORME TÉCNICO"
    df["DOCUMENTO"] = "IT"
    df["REPORTE"] = df["OBSERVACIONES"].fillna(df["Informe"])
    
    # 🆔 ID único
    df["id_unico"] = (df["FECHA"].dt.strftime('%Y-%m-%d') + "_" + 
                      df["SERIE"] + "_" + 
                      df["DOCUMENTO"] + "_" + 
                      df["TIPO"])
    
    # ✅ Columnas FINALES en orden EXACTO
    cols = ['FECHA', 'TIPO', 'REPORTE', 'DOCUMENTO', 'SERIE', 'id_unico']
    df_final = df[cols].copy()

    save_csv(df_final.reset_index(drop=True), "it_processed.csv")


def filter_by_catastro():
    """🔍 Cruza TODOS los procesados con Catastro → SOBREESCRIBE con series válidas"""
    catastro_path = os.path.join(raw_dir, "catastro_raw.csv")
    if not os.path.exists(catastro_path):
        logging.warning("❌ catastro_raw.csv no encontrado")
        return
    
    # 📦 Carga catastro y extrae series válidas
    catastro = pd.read_csv(catastro_path, dtype=str)
    series_validas = set(catastro["SERIE"].dropna().astype(str).str.strip().str.upper())
    logging.info(f"✅ Series válidas en catastro: {len(series_validas):,}")
    
    # 📋 Archivos a filtrar (los que usan dividir_y_agregar)
    processed_files = [
        "ae_processed.csv", "ap_processed.csv", "cs_processed.csv", 
        "ot2025_processed.csv", "hdv_im_processed.csv", "it_processed.csv"
    ]
    
    total_original = 0
    total_filtrada = 0
    
    for filename in processed_files:
        processed_path = os.path.join(processed_dir, filename)
        if not os.path.exists(processed_path):
            logging.warning(f"⚠️  {filename} no existe")
            continue
        
        # 📖 Lee processed
        df = pd.read_csv(processed_path, dtype=str)
        if df.empty or "SERIE" not in df.columns:
            logging.warning(f"⚠️  {filename} vacío o sin SERIE")
            continue
        
        original_count = len(df)
        total_original += original_count
        
        # 🔥 FILTRA solo series EN CATSTRO
        df_filtrada = df[df["SERIE"].str.strip().str.upper().isin(series_validas)].copy()
        filtrada_count = len(df_filtrada)
        total_filtrada += filtrada_count
        
        # 💾 SOBREESCRIBE el processed con datos filtrados
        if filtrada_count > 0:
            df_filtrada.to_csv(processed_path, index=False, encoding='utf-8')
            pct_match = (filtrada_count / original_count) * 100
            logging.info(f"🔎 {filename}: {original_count:,} → {filtrada_count:,} ({pct_match:.1f}% match)")
        else:
            # Borra archivo vacío
            os.remove(processed_path)
            logging.warning(f"🗑️  {filename}: 0 coincidencias → ELIMINADO")
    
    logging.info(f"🎯 CRUCE COMPLETO: {total_original:,} → {total_filtrada:,} filas válidas")
    

# =============================================================================
# 🔥 EJECUCIÓN PRINCIPAL
# =============================================================================
if __name__ == "__main__":
    start_time = time.time()
    
    logging.info("🚀 1/2 PROCESAMIENTO RAW → PROCESSED (sin filtro)")
    processors = [
        process_pmp, process_pmp_im_mayor_12, process_ae, process_ap, 
        process_cs, process_catastro, process_ot, process_hdv_im, 
        process_amfe, process_it
    ]
    
    for proc in processors:
        proc()
    
    logging.info("🔍 2/2 CRUCE CON CATSTRO → SOBREESCRIBE processed")
    filter_by_catastro()
    
    end_time = time.time()
    logging.info("✅ PIPELINE 100% COMPLETO")
    print(f"\n⏱️  Tiempo total: {end_time - start_time:.2f}s")
    print("📁 data/processed/ → SOLO SERIES VÁLIDAS DEL CATSTRO ✅")
    
    # 🔥 CRUCE CON CASTRATO (nuevo!)
    logging.info("🔍 Filtrando por series válidas de Catastro...")
    filter_by_catastro()
    
    end_time = time.time()
    logging.info("✅ PIPELINE COMPLETO")
    print(f"\n⏱️  Tiempo total: {end_time - start_time:.2f} segundos")
    print("📁 data/processed/ → SOLO series válidas del Catastro")