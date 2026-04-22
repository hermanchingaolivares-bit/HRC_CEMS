# -*- coding: utf-8 -*-
"""
Created on Sat Dec 20 00:26:23 2025

@author: herma
"""
import pandas as pd
import logging
from datetime import datetime
import re

def convertir_fecha_estandar(fecha_input):
    if pd.isna(fecha_input) or str(fecha_input).strip() == '':
        return pd.NaT
    if isinstance(fecha_input, (datetime, pd.Timestamp)):
        return pd.to_datetime(fecha_input)

    fecha_str = str(fecha_input).strip()
    # Eliminar cualquier carácter que no sea dígito, '/', '-' o espacio
    # Esto quita puntos y otros caracteres no esperados
    fecha_str = re.sub(r'[^0-9/\-\s]', '', fecha_str)

    formatos = [
        '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%d-%m-%Y %H:%M:%S', '%Y/%m/%d %H:%M:%S',
        '%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d', '%m/%d/%Y', '%m-%d-%Y', '%Y%m%d', '%d%m%Y'
    ]

    for fmt in formatos:
        try:
            return pd.to_datetime(fecha_str, format=fmt, errors='raise')
        except ValueError:
            continue

    # Último intento: inferir fecha automáticamente, día primero
    try:
        return pd.to_datetime(fecha_str, dayfirst=True, errors='coerce')
    except Exception as e:
        logging.warning(f"No se pudo convertir la fecha '{fecha_input}': {e}")
        return pd.NaT


def dividir_y_agregar(texto):
    if pd.isna(texto):
        return []
    lista_espacios = str(texto).split(" ")
    lista_final = []
    for item in lista_espacios:
        if ":" in item:
            partes = item.split(":")
        elif "//" in item:
            partes = item.split("//")
        elif "/" in item:
            partes = item.split("/")
        else:
            partes = [item]
        for parte in partes:
            parte_limpia = parte.strip()
            if parte_limpia.endswith("."):
                parte_limpia = parte_limpia[:-1]
            if parte_limpia:
                lista_final.append(parte_limpia)
    return lista_final

def estandarizar_clave(clave):
    if pd.isna(clave):
        return ""
    return str(clave).strip().replace(" ", "").upper()

def es_nic_valido(nic):
    nic_str = str(nic).strip()
    if nic_str in ('0', ',', '', 'NAN', 'NONE'):
        return False
    bloqueos = ['APA-', 'AP-', 'ASSET', 'COD']
    if any(x in nic_str.upper() for x in bloqueos):
        return False
    return True