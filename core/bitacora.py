"""Registro de actividad del ETL.

Cada corrida deja dos rastros: uno en la base de datos (las tablas `carga` y
`rechazo`) y otro en un archivo de texto, que es el que sirve cuando la
corrida falla antes de poder escribir en la base.
"""

import logging
from datetime import datetime
from pathlib import Path

from core.config import obtener_ajustes
from core.rutas import BITACORAS as DIRECTORIO_BITACORAS
from core.rutas import asegurar_directorio

_FORMATO = "%(asctime)s  %(levelname)-8s %(name)s  %(message)s"


def configurar_bitacora(nombre_corrida: str) -> logging.Logger:
    """Deja el registro escribiendo en pantalla y en `logs/`.

    El archivo lleva la fecha y la hora en el nombre para que una corrida no
    pise el registro de la anterior: cuando algo sale mal en el hospital,
    interesa el registro de esa corrida, no el del ultimo intento.
    """
    ajustes = obtener_ajustes()
    asegurar_directorio(DIRECTORIO_BITACORAS)

    marca = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo = DIRECTORIO_BITACORAS / f"{nombre_corrida}_{marca}.log"

    logging.basicConfig(
        level=ajustes.nivel_bitacora.upper(),
        format=_FORMATO,
        handlers=[logging.StreamHandler(), logging.FileHandler(archivo, encoding="utf-8")],
        force=True,
    )

    registro = logging.getLogger(nombre_corrida)
    registro.info("Registro de esta corrida: %s", archivo)
    return registro


def ruta_ultima_bitacora() -> Path | None:
    """Devuelve el archivo de registro mas reciente, si hay alguno."""
    if not DIRECTORIO_BITACORAS.is_dir():
        return None
    archivos = sorted(DIRECTORIO_BITACORAS.glob("*.log"))
    return archivos[-1] if archivos else None
