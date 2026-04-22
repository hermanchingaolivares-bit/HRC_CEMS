# -*- coding: utf-8 -*-
"""
Created on Sun Dec 21 20:58:56 2025

@author: herma
"""

import os
import pandas as pd
from collections import Counter
import logging
import time
from dotenv import load_dotenv
from utils import convertir_fecha_estandar, estandarizar_clave, es_nic_valido  # Ajusta el import según tu estructura

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definir ruta raíz y carpeta para guardar datos procesados
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
processed_dir = os.path.join(project_root, 'data/processed')
os.makedirs(processed_dir, exist_ok=True)

# Cargar variables de entorno
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path)

def procesar_hoja_mantenimiento(df_hoja):
    mapa_tipos = {
        "MANT. CORRECTIVO": "MANTENIMIENTO_CORRECTIVO",
        "MANT. PREVENTIVO": "MANTENIMIENTO_PREVENTIVO",
        "": "SIN_TIPO",
        "NAN": "SIN_TIPO",
        "NONE": "SIN_TIPO"
    }
    for fila_idx in range(5, 11):
        if fila_idx >= len(df_hoja) or df_hoja.iloc[fila_idx].isnull().all():
            continue

        encabezados = df_hoja.iloc[fila_idx].astype(str).str.upper().values
        tiene_fecha = any('FECHA' in c for c in encabezados)
        tiene_mc_mp = any('MC' in c or 'MP' in c for c in encabezados)

        if not (tiene_fecha and tiene_mc_mp):
            continue

        try:
            df_temp = df_hoja.copy()
            df_temp.columns = df_temp.iloc[fila_idx]

            datos = df_temp.iloc[fila_idx+1:].dropna(subset=["FECHA"]).reset_index(drop=True)
            if datos.empty:
                continue

            datos.columns = [str(col).strip().upper() for col in datos.columns]
            datos = datos.loc[:, ~datos.columns.duplicated()].copy()

            datos['TIPO'] = datos.apply(
                lambda r: "MANT. CORRECTIVO" if str(r.get('MC', '')).strip().upper() == "X"
                else "MANT. PREVENTIVO" if str(r.get('MP', '')).strip().upper() == "X"
                else "",
                axis=1
            )
            datos['TIPO'] = datos['TIPO'].map(mapa_tipos).fillna("TIPO_DESCONOCIDO")

            if 'ACTIVIDAD' not in datos.columns:
                datos['ACTIVIDAD'] = ''

            datos["REPORTE"] = datos['ACTIVIDAD'].astype(str).fillna('')
            if 'OBS' in datos.columns:
                datos['REPORTE'] += datos['OBS'].astype(str).fillna('').apply(lambda x: f" {x}" if x else "")
            datos["REPORTE"] = datos["REPORTE"].str.strip()

            documento_col_name = next((col for col in datos.columns if 'DOCUMENTO' in col or 'LINK' in col), None)
            if documento_col_name:
                datos["DOCUMENTO_DIGITAL"] = datos[documento_col_name].astype(str).fillna('')
            elif len(datos.columns) > 1:
                datos["DOCUMENTO_DIGITAL"] = datos.iloc[:, 1].astype(str).fillna('')
            else:
                datos["DOCUMENTO_DIGITAL"] = ''

            return datos[["FECHA", 'TIPO', 'REPORTE', 'DOCUMENTO_DIGITAL']].rename(
                columns={'TIPO': 'tipo', 'REPORTE': 'reporte', 'DOCUMENTO_DIGITAL': 'documento_digital'}
            )
        except Exception as e:
            logging.error(f"Error procesando hoja en fila {fila_idx}: {e}")
            continue
    return None

def raw_hdv():
    start_time = time.time()

    excel_path = os.getenv('EXCEL_HOJA_DE_VIDA_PATH')

    if not excel_path or not os.path.exists(excel_path):
        logging.error(f"Archivo no encontrado en la ruta configurada: {excel_path}")
        return pd.DataFrame()

    logging.info(f'Abriendo archivo Excel en: {excel_path}')

    try:
        hdv = pd.ExcelFile(excel_path)
    except Exception as e:
        logging.error(f"Error al abrir {excel_path}: {e}")
        return pd.DataFrame()

    sheets = {}
    for hoja in hdv.sheet_names:
        try:
            sheets[estandarizar_clave(hoja)] = hdv.parse(hoja)
        except Exception as e:
            logging.warning(f"No se pudo parsear hoja '{hoja}': {e}")
    hdv.close()

    listado_key = estandarizar_clave('EQUIPOSCRITICOS2019')
    listado = sheets.get(listado_key, None)
    if listado is None:
        logging.error(f"No se encontró la hoja '{listado_key}' en el archivo")
        return pd.DataFrame()
    if listado.empty or len(listado) < 5:
        logging.error("La hoja equipos críticos no tiene suficientes filas")
        return pd.DataFrame()

    listado_hdv = listado.iloc[4:].reset_index(drop=True)
    listado_hdv.columns = listado_hdv.iloc[0]
    listado_hdv = listado_hdv.iloc[1:].reset_index(drop=True)

    listado_hdv.columns = [str(c).strip().upper() for c in listado_hdv.columns]
    listado_hdv = listado_hdv.loc[:, ~listado_hdv.columns.duplicated()].copy()

    required_cols = ["EQUIPO", "NIC", "SERIE"]
    if not all(col in listado_hdv.columns for col in required_cols):
        missing = [c for c in required_cols if c not in listado_hdv.columns]
        logging.error(f"Faltan columnas en listado: {missing}")
        return pd.DataFrame()

    listado_hdv = listado_hdv[required_cols].dropna(subset=["NIC"])
    listado_hdv["NIC_ESTANDAR"] = listado_hdv["NIC"].apply(estandarizar_clave)

    hdv_final = pd.DataFrame()
    nics_no_procesados = []
    errores = []
    nics_validos = []

    for _, row in listado_hdv.iterrows():
        nic = estandarizar_clave(str(row["NIC"]))
        serie = str(row.get("SERIE", "")).strip().upper()

        if not es_nic_valido(nic):
            nics_no_procesados.append(row["NIC"])
            continue

        if nic not in sheets:
            nics_no_procesados.append(row["NIC"])
            errores.append(f"NIC {row['NIC']}: Hoja no encontrada")
            continue

        try:
            hdv_temporal = sheets[nic].copy()
            if not hdv_temporal.empty and hdv_temporal.iloc[:, 0].isnull().all():
                hdv_temporal = hdv_temporal.iloc[:, 1:]

            datos = procesar_hoja_mantenimiento(hdv_temporal)

            if datos is not None and not datos.empty:
                datos["NIC"] = nic
                datos["SERIE"] = serie
                hdv_final = pd.concat([hdv_final, datos], ignore_index=True)
                nics_validos.append(row["NIC"])
            else:
                nics_no_procesados.append(row["NIC"])
                errores.append(f"NIC {row['NIC']}: Estructura de datos no válida o vacía")
        except Exception as e:
            nics_no_procesados.append(row["NIC"])
            errores.append(f"NIC {row['NIC']}: Error inesperado - {e}")

    hdv_final.drop_duplicates(subset=["NIC", "FECHA", "reporte"], inplace=True)
    hdv_final["FECHA"] = hdv_final["FECHA"].apply(convertir_fecha_estandar)
    hdv_final.dropna(subset=["FECHA"], inplace=True)
    hdv_final.reset_index(drop=True, inplace=True)

    orden_tipos = ["MANTENIMIENTO_PREVENTIVO", "MANTENIMIENTO_CORRECTIVO", "SIN_TIPO", "TIPO_DESCONOCIDO"]
    hdv_final["tipo"] = pd.Categorical(hdv_final["tipo"], categories=orden_tipos, ordered=True)
    hdv_final = hdv_final.sort_values("tipo").reset_index(drop=True)

    logging.info("Resumen procesamiento HDV RAW:")
    logging.info(f"- Registros procesados: {len(hdv_final)}")
    logging.info(f"- Equipos procesados: {len(set(nics_validos))}")
    logging.info(f"- NICs no procesados: {len(set(nics_no_procesados))}")
    logging.info(f"- Errores totales: {len(errores)}")

    if nics_no_procesados:
        logging.info(f"Ejemplos NICs no procesados: {list(set(nics_no_procesados))[:10]}")

    if errores:
        logging.info("Errores comunes:")
        errores_comunes = Counter([err.split(":")[-1].strip() for err in errores])
        for error_text, count in errores_comunes.most_common(5):
            logging.info(f"  {error_text} -- {count} veces")

        reporte_path = os.path.join(processed_dir, "reporte_problemas_raw_hdv.txt")
        with open(reporte_path, "w", encoding="utf-8") as f:
            for err in errores:
                f.write(err + "\n")
        logging.info(f"Reporte de errores guardado en '{reporte_path}'")

    logging.info("----- FIN PROCESAMIENTO HOJAS DE VIDA -----")

    hdv_final = hdv_final.rename(columns={"tipo": "TIPO", "reporte": "REPORTE", "documento_digital": "DOCUMENTO"})
    hdv_final["TIPO"] = hdv_final["TIPO"].astype(str).fillna("")

    hdv_final["id_unico"] = (
        hdv_final["FECHA"].dt.strftime("%Y-%m-%d").fillna("")
        + "_" + hdv_final["SERIE"].astype(str).fillna("")
        + "_" + hdv_final["DOCUMENTO"].astype(str).str.strip().str.upper().fillna("")
        + "_" + hdv_final["TIPO"]
    )

    output_csv = os.path.join(processed_dir, "hdv_processed.csv")
    hdv_final.to_csv(output_csv, index=False)
    logging.info(f"Datos procesados guardados en '{output_csv}'")

    end_time = time.time()
    logging.info(f"Tiempo total de procesamiento: {end_time - start_time:.2f} segundos")

    return hdv_final

if __name__ == "__main__":
    raw_hdv()