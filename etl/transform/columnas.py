"""Busqueda de columnas tolerante a como esten escritos los encabezados.

Las planillas escriben el mismo encabezado de varias formas: `FUNCIÓN` y
`FUNCION`, `Servicio` y `SERVICIO`, con espacios de mas o con un salto de
linea adentro. Buscar por el texto exacto haria que el ETL se cayera cada vez
que alguien corrige una tilde.

La regla es simple: para comparar, los encabezados se pasan a mayusculas, se
les quitan las tildes y se colapsan los espacios. El nombre original de la
columna no se toca; solo cambia la forma de buscarla.
"""

import re

from unidecode import unidecode

_ESPACIOS = re.compile(r"\s+")


def clave(nombre: object) -> str:
    """Forma comparable de un nombre de columna."""
    if nombre is None:
        return ""
    return _ESPACIOS.sub(" ", unidecode(str(nombre)).strip().upper())


def buscar(fila: dict[str, str], *alternativas: str) -> str | None:
    """Devuelve el valor de la primera columna que exista en la fila.

    Se le pasan las formas en que la unidad puede haber escrito el encabezado:

        buscar(fila, "RIESGO FISICO", "RIESGO")
    """
    indexada = {clave(columna): valor for columna, valor in fila.items()}
    for alternativa in alternativas:
        valor = indexada.get(clave(alternativa))
        if valor is not None:
            return valor
    return None


def exigir_columnas(fila: dict[str, str], *obligatorias: str) -> None:
    """Falla si la planilla no trae una columna imprescindible.

    Vale la pena cortar aca: si cambio el encabezado, cargar igual con esa
    columna vacia dejaria datos incompletos sin que nadie se entere.
    """
    presentes = {clave(columna) for columna in fila}
    faltantes = [columna for columna in obligatorias if clave(columna) not in presentes]
    if faltantes:
        raise ColumnaFaltante(
            f"La planilla no trae la columna {', '.join(faltantes)}. "
            f"Encabezados leidos: {sorted(fila)[:15]}"
        )


class ColumnaFaltante(KeyError):
    """Falta una columna imprescindible en la planilla."""
