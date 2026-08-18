"""Donde se guarda cada cosa.

Un solo lugar decide las rutas de salida. Ningun modulo arma rutas por su
cuenta: si manana cambia la organizacion de `data/`, se cambia aca y nada mas.

    data/raw/         lo que se leyo, tal cual salio de la fuente
    data/interim/     lo mismo ya normalizado, antes de decidir que entra
    data/processed/   lo que efectivamente se cargo a la base
    data/backups/     respaldos de la base
    logs/             el registro de cada corrida
    reports/          reportes para la unidad

El contenido de esas carpetas **no se versiona**; la estructura si, con un
archivo `.gitkeep`. Por eso `asegurar_directorio` lo crea al vuelo: si alguien
agrega una carpeta nueva, git la conserva sin tener que acordarse.

Por que existen las tres etapas: `raw` permite repetir un analisis sin volver
a pedirle los datos a Google, `interim` deja ver que hizo la normalizacion sin
mezclarlo con lo que se cargo, y `processed` es lo que se puede comparar
contra la base. El prototipo escribia encima del mismo archivo en cada paso,
asi que cuando algo salia mal no habia con que comparar.
"""

from pathlib import Path

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent

DATOS = RAIZ_PROYECTO / "data"

CRUDOS = DATOS / "raw"
CRUDOS_GOOGLE = CRUDOS / "google_sheets"
CRUDOS_EXCEL = CRUDOS / "excel"

INTERMEDIOS = DATOS / "interim"
PROCESADOS = DATOS / "processed"
RESPALDOS = DATOS / "backups"

BITACORAS = RAIZ_PROYECTO / "logs"
REPORTES = RAIZ_PROYECTO / "reports"

SECRETOS = RAIZ_PROYECTO / "secrets"
ESQUEMA = RAIZ_PROYECTO / "database" / "schema"
MIGRACIONES = RAIZ_PROYECTO / "database" / "migrations"


def asegurar_directorio(directorio: Path) -> Path:
    """Crea el directorio si falta y le deja su `.gitkeep`."""
    directorio.mkdir(parents=True, exist_ok=True)
    if _es_ignorado(directorio):
        marca = directorio / ".gitkeep"
        if not marca.exists():
            marca.touch()
    return directorio


def _es_ignorado(directorio: Path) -> bool:
    """True si el contenido del directorio no se versiona."""
    return any(
        directorio == raiz or raiz in directorio.parents for raiz in (DATOS, BITACORAS, REPORTES)
    )


def archivo_crudo(fuente: str, extension: str = "csv") -> Path:
    """Ruta donde dejar lo leido de una fuente, sin transformar."""
    return asegurar_directorio(CRUDOS_GOOGLE) / f"{_nombre_de_archivo(fuente)}.{extension}"


def archivo_intermedio(fuente: str, extension: str = "csv") -> Path:
    """Ruta donde dejar una fuente ya normalizada."""
    return asegurar_directorio(INTERMEDIOS) / f"{_nombre_de_archivo(fuente)}.{extension}"


def archivo_procesado(fuente: str, extension: str = "csv") -> Path:
    """Ruta donde dejar lo que se cargo a la base."""
    return asegurar_directorio(PROCESADOS) / f"{_nombre_de_archivo(fuente)}.{extension}"


def _nombre_de_archivo(fuente: str) -> str:
    """Convierte el nombre de una hoja en un nombre de archivo tratable.

    'PMP IM>12' no sirve como nombre de archivo en Windows, asi que queda
    como 'pmp_im_12'.
    """
    limpio = "".join(letra if letra.isalnum() else "_" for letra in fuente.lower())
    while "__" in limpio:
        limpio = limpio.replace("__", "_")
    return limpio.strip("_")
