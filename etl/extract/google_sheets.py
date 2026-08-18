"""Lectura del Google Sheet "EEMM" por la API.

Siempre por la API y siempre como texto. Nunca desde un archivo exportado:
al exportar, las series numericas largas se convierten a notacion cientifica
y quedan corrompidas sin que se note.

Como texto significa que aca no se interpreta nada. Una fecha llega como la
escribio la unidad y un monto llega con sus puntos y su signo peso. Convertir
es tarea de `etl/transform`, y hacerlo alla permite reportar lo que no se
entiende en vez de perderlo.
"""

import csv
import logging
from pathlib import Path
from typing import Any

from core.config import obtener_ajustes
from core.rutas import archivo_crudo

# La service account solo necesita leer, pero la API de Sheets exige tambien
# el alcance de Drive para poder abrir el libro por su nombre.
_ALCANCES = (
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
)

_registro = logging.getLogger(__name__)


class HojaNoEncontrada(LookupError):
    """La hoja pedida no existe en el libro.

    Es un error y se levanta como tal. El prototipo devolvia una tabla vacia,
    asi que una hoja mal escrita se veia igual que una hoja sin datos: llevaba
    anios leyendo "AMFE EQUIPOS", que no existe, sin que nadie lo supiera.
    """


def abrir_libro() -> Any:
    """Abre el libro de Google con la service account del .env."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as error:  # pragma: no cover - depende del entorno
        raise RuntimeError(
            "Falta la libreria gspread. Instalala con:\n"
            '    C:\\Users\\herma\\anaconda3\\envs\\cems\\python.exe -m pip install gspread "psycopg[binary]"'
        ) from error

    ajustes = obtener_ajustes()
    credenciales = Credentials.from_service_account_file(
        str(ajustes.exigir_credenciales_google()), scopes=list(_ALCANCES)
    )
    cliente = gspread.authorize(credenciales)
    _registro.info("Abriendo el libro de Google '%s'", ajustes.nombre_libro_google)
    return cliente.open(ajustes.nombre_libro_google)


def leer_hoja(libro: Any, nombre: str, fila_encabezado: int = 1) -> list[dict[str, str]]:
    """Devuelve una hoja como lista de diccionarios, todo en texto.

    `fila_encabezado` se cuenta desde 1, como lo muestra la planilla.
    """
    import gspread

    try:
        hoja = libro.worksheet(nombre)
    except gspread.WorksheetNotFound as error:
        disponibles = ", ".join(sorted(h.title for h in libro.worksheets()))
        raise HojaNoEncontrada(
            f"La hoja '{nombre}' no existe en el libro. Hojas disponibles: {disponibles}"
        ) from error

    valores = hoja.get_all_values()
    if len(valores) <= fila_encabezado:
        _registro.warning("La hoja '%s' no tiene filas bajo su encabezado", nombre)
        return []

    encabezados = _nombrar_columnas(valores[fila_encabezado - 1])
    filas = [
        dict(zip(encabezados, _completar(fila, len(encabezados)), strict=True))
        for fila in valores[fila_encabezado:]
    ]

    _registro.info("Hoja '%s': %s filas leidas", nombre, len(filas))
    return filas


def _nombrar_columnas(encabezado: list[str]) -> list[str]:
    """Deja nombres de columna utilizables aunque la planilla no ayude.

    Las planillas tienen columnas sin titulo y titulos repetidos. En vez de
    fallar, se les da un nombre estable para poder referirse a ellas.
    """
    nombres: list[str] = []
    for posicion, titulo in enumerate(encabezado, start=1):
        nombre = titulo.strip() or f"columna_{posicion}"
        if nombre in nombres:
            nombre = f"{nombre}_{posicion}"
        nombres.append(nombre)
    return nombres


def _completar(fila: list[str], ancho: int) -> list[str]:
    """Iguala el largo de la fila al del encabezado.

    Google recorta las celdas vacias del final, asi que las filas cortas son
    lo normal, no un error.
    """
    return fila[:ancho] + [""] * (ancho - len(fila))


def nombre_hoja_ot(anio: int) -> str:
    """Nombre de la hoja de ordenes de trabajo del anio: 2026 -> 'OT26'.

    Los nombres de hoja cambian con el anio, asi que se calculan y no se
    escriben como constante en ninguna parte.
    """
    return f"OT{anio % 100:02d}"


def guardar_copia_cruda(filas: list[dict[str, str]], nombre: str) -> Path | None:
    """Guarda en `data/raw/` lo que se leyo, como evidencia de la corrida.

    No es una fuente: sirve para revisar un problema sin volver a pedirle los
    datos a Google. El ETL nunca lee de aca.
    """
    if not filas:
        return None

    archivo = archivo_crudo(nombre)

    with archivo.open("w", encoding="utf-8", newline="") as salida:
        escritor = csv.DictWriter(salida, fieldnames=list(filas[0].keys()))
        escritor.writeheader()
        escritor.writerows(filas)

    _registro.info("Copia cruda de '%s' guardada en %s", nombre, archivo)
    return archivo
