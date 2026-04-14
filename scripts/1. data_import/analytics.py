# -*- coding: utf-8 -*-
"""
EEMM ANALYTICS - CSV DIRECTO (funciones independientes)
Carga CSV raw → Análisis → CSV analytics
@author: profesional.eemm
"""
import os
import pandas as pd
import logging
from utils import (dividir_y_agregar)
# CONFIG (rápida)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
raw_dir = os.path.join(project_root, 'data/raw/google_sheets')
analytics_dir = os.path.join(project_root, 'data/processed/analytics')
os.makedirs(analytics_dir, exist_ok=True)

def load_csv(name):
    """Carga CSV raw (1 línea)"""
    path = os.path.join(raw_dir, name)
    try:
        df = pd.read_csv(path)
        print(f"✓ {name}: {len(df)} filas")
        return df
    except Exception as e:
        print(f"✗ {name}: {e}")
        return pd.DataFrame()

def save_csv(df, name):
    """Guarda analytics"""
    path = os.path.join(analytics_dir, name)
    df.to_csv(path, index=False)
    print(f"💾 {analytics_dir}/{name}")

# 🔥 FUNCIÓN 1: OT por MES/AÑO (tu función original mejorada)
def ot_por_año():
    """OT → Tabla MES/AÑO (rápido!)"""
    print("\n📊 === OT POR AÑO/MES ===")
    
    # CARGAR SOLO OT
    ot = load_csv('ot_raw.csv')
    if ot.empty: return
    print(ot.columns)
    # LIMPIAR
    ot_limpio = ot.drop_duplicates(subset=['OT']).copy()
    ot_limpio['FECHA'] = pd.to_datetime(ot_limpio['FECHA'], format='%d/%m/%Y', errors='coerce')    
    ot_limpio['AÑO'] = ot_limpio['FECHA'].dt.year
    ot_limpio['MES_NOMBRE'] = ot_limpio['FECHA'].dt.strftime('%b')
    ot_filtrado = ot_limpio[ot_limpio['AÑO'].isin([2023, 2024, 2025, 2026])]
    
    print(f"✅ OT válidos: {len(ot_filtrado)}")
    
    # ANUAL
    anual = ot_filtrado['AÑO'].value_counts().sort_index()
    print("\n📅 POR AÑO:")
    print(anual)
    
    # MES/AÑO
    tabla = ot_filtrado.pivot_table(
        index='MES_NOMBRE', columns='AÑO', 
        values='OT', aggfunc='count', fill_value=0
    ).astype(int)
    
    meses = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    tabla = tabla.reindex(meses)
    tabla['TOTAL'] = tabla.sum(axis=1)
    tabla.loc['TOTAL'] = tabla.sum()
    
    print("\n📊 TABLA MES/AÑO:")
    print(tabla)
    
    # GUARDAR
    save_csv(tabla, 'ot_mes_ano.csv')

    return tabla

def ot_equipos():
    catastro = load_csv('catastro_raw.csv')
    if catastro.empty: return pd.DataFrame()
    
    catastro = catastro.dropna(subset=['SERIE'])
    catastro["SERIE_NORMALIZADA"] = catastro["SERIE"].astype(str).str.strip().str.upper()
    catastro = catastro[catastro["SERIE_NORMALIZADA"] != ""]
    
    ot = load_csv('ot_raw.csv')
    if ot.empty: return pd.DataFrame()
    
    ot_cerradas = ot[ot['ESTADO'] == 'Cerrada'].copy()
    
    ot_cerradas["SERIE_ORIGINAL"] = ot_cerradas["IDENTIFICACIÓN DEL EQUIPO"].astype(str).str.strip().str.upper()
    ot_cerradas["SERIES"] = ot_cerradas["SERIE_ORIGINAL"].apply(dividir_y_agregar)
    
    ot_exploded = ot_cerradas.explode("SERIES").reset_index(drop=True)
    ot_exploded["SERIE_NORMALIZADA"] = ot_exploded["SERIES"].astype(str).str.strip().str.upper()
    ot_exploded = ot_exploded[ot_exploded["SERIE_NORMALIZADA"].notna() & (ot_exploded["SERIE_NORMALIZADA"] != "")]
    
    merged = ot_exploded.merge(
        catastro[['SERIE_NORMALIZADA', 'SERIE', 'NOMBRE EQUIPO', 'MARCA', 'MODELO']],
        on='SERIE_NORMALIZADA',
        how='inner'
    )
    
    # DF FINAL con UNIDAD
    df_final = merged[['UNIDAD', 'FECHA', 'OT', 'SERIE', 'NOMBRE EQUIPO', 'MARCA', 'MODELO', 'OBS CLÍNICA', 'OBS EEMM']].drop_duplicates()
    
    # CANTIDAD por SERIE
    df_final['CANTIDAD'] = df_final.groupby('SERIE')['OT'].transform('count')
    
    # ORDEN columnas
    df_final = df_final[['SERIE', 'NOMBRE EQUIPO', 'MARCA', 'MODELO', 'UNIDAD', 'FECHA', 'OT', 'CANTIDAD', 'OBS CLÍNICA', 'OBS EEMM']]
    
    print(f"OT Cerradas: {len(ot_cerradas)} filas")
    print(f"Coincidencias: {len(merged)}")
    print(f"DF final: {len(df_final)} filas")
    
    return df_final



if __name__ == "__main__":
    # ot_año_mes=ot_por_año()
    ot_equipos = ot_equipos()
