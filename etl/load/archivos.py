"""Copia en disco de lo que se cargo a la base.

`data/processed/` guarda exactamente las filas que entraron. Sirve para
comparar contra la base sin escribir una consulta, y para ver que cambio de
una corrida a otra.

No es una fuente ni un respaldo: la base es la que manda. Si el archivo y la
base no coinciden, manda la base.
"""

import csv
import logging
from dataclasses import asdict
from pathlib import Path

from core.rutas import archivo_procesado

_registro = logging.getLogger(__name__)


def guardar_procesado(filas: list, fuente: str) -> Path | None:
    """Escribe en `data/processed/` las filas cargadas de una fuente."""
    if not filas:
        return None

    diccionarios = [asdict(fila) for fila in filas]
    ruta = archivo_procesado(fuente)

    with ruta.open("w", encoding="utf-8", newline="") as salida:
        escritor = csv.DictWriter(salida, fieldnames=list(diccionarios[0].keys()))
        escritor.writeheader()
        escritor.writerows(diccionarios)

    _registro.info("Copia de lo cargado en %s", ruta)
    return ruta
