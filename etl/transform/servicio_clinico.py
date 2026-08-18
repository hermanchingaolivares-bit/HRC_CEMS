"""De las hojas Datos_Unidades y Agenda al catalogo `servicio_clinico`.

Son dos hojas con dos papeles distintos:

    Datos_Unidades  ->  que unidades existen. Manda esta.
    Agenda          ->  como contactarlas: responsable, correo y anexo.

La agenda tiene varias personas por unidad, asi que como responsable se toma
la que tenga jefatura en el cargo. Si ninguna la tiene, la unidad se carga
igual pero sin contacto: perder la unidad seria peor que no tener su telefono.

Una unidad que aparece en la agenda y no esta en el listado no se inventa: se
cuenta como ignorada. El listado es el que define que unidades existen.
"""

import logging
from dataclasses import dataclass

from etl.transform.columnas import buscar, clave, exigir_columnas
from etl.transform.normalizar import normalizar_texto
from etl.transform.resultado import NOMBRE_DUPLICADO, Resultado

FUENTE = "Datos_Unidades + Agenda"

HOJA_UNIDADES = "Datos_Unidades"
HOJA_AGENDA = "Agenda"

# En que fila esta el encabezado de cada hoja, contando como lo muestra la
# planilla. La agenda lleva su titulo arriba, asi que el encabezado va segundo.
# Leerla con la fila equivocada dejaria a todas las unidades sin contacto, por
# eso el numero vive aca y no en el script que la lee.
FILA_ENCABEZADO_UNIDADES = 1
FILA_ENCABEZADO_AGENDA = 2

# Cargos que se consideran jefatura, en orden de preferencia.
_CARGOS_DE_JEFATURA = ("JEFE", "JEFA", "ENCARGADO", "ENCARGADA", "COORDINADOR", "COORDINADORA")

_registro = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServicioClinico:
    """Una fila lista para `servicio_clinico`."""

    nombre: str
    responsable: str | None
    anexo: str | None
    correo: str | None


@dataclass(frozen=True)
class _Contacto:
    responsable: str | None
    anexo: str | None
    correo: str | None


def transformar(
    unidades: list[dict[str, str]], agenda: list[dict[str, str]]
) -> Resultado[ServicioClinico]:
    """Cruza el listado de unidades con la agenda de contactos."""
    resultado: Resultado[ServicioClinico] = Resultado(leidas=len(unidades))

    contactos = _contactos_por_unidad(agenda)
    usadas: set[str] = set()
    vistas: dict[str, str] = {}

    for fila in unidades:
        nombre = normalizar_texto(_nombre_de_unidad(fila))
        if nombre is None:
            resultado.ignorar()
            continue

        llave = clave(nombre)
        if llave in vistas:
            resultado.rechazar(
                NOMBRE_DUPLICADO,
                valor=nombre,
                detalle=f"La unidad ya aparecio antes como '{vistas[llave]}'. Se carga una sola vez",
            )
            continue
        vistas[llave] = nombre

        contacto = contactos.get(llave)
        if contacto is not None:
            usadas.add(llave)

        resultado.aceptar(
            ServicioClinico(
                nombre=nombre,
                responsable=contacto.responsable if contacto else None,
                anexo=contacto.anexo if contacto else None,
                correo=contacto.correo if contacto else None,
            )
        )

    sobrantes = sorted(set(contactos) - usadas)
    if sobrantes:
        resultado.ignorar(len(sobrantes))
        _registro.info(
            "%s unidades de la agenda no estan en el listado y no se cargan: %s",
            len(sobrantes),
            ", ".join(sobrantes[:10]),
        )

    _registro.info("%s: %s", FUENTE, resultado.resumen())
    return resultado


def _nombre_de_unidad(fila: dict[str, str]) -> str | None:
    """El nombre de la unidad, venga con el encabezado que venga.

    `Datos_Unidades` es una sola columna, y su titulo ha cambiado de nombre.
    Si no coincide con ninguno de los conocidos, se toma la primera columna.
    """
    valor = buscar(fila, "LISTADO DE UNIDADES", "UNIDAD", "UNIDADES", "SERVICIO", "SERVICIO CLINICO")
    if valor is not None:
        return valor
    for primero in fila.values():
        return primero
    return None


def _contactos_por_unidad(agenda: list[dict[str, str]]) -> dict[str, _Contacto]:
    """Un contacto por unidad, prefiriendo a quien tenga jefatura."""
    mejores: dict[str, tuple[int, _Contacto]] = {}
    if not agenda:
        return mejores

    # Si la agenda se leyo con la fila de encabezado equivocada, sus columnas
    # se llaman 'columna_2', 'columna_3'... y ninguna busqueda encontraria nada:
    # todas las unidades quedarian sin contacto y nadie se enteraria. Mejor
    # cortar y decir que paso.
    exigir_columnas(agenda[0], "UNIDAD", "CARGO", "NOMBRE")

    for fila in agenda:
        unidad = normalizar_texto(buscar(fila, "UNIDAD", "SERVICIO", "SERVICIO CLINICO"))
        if unidad is None:
            continue

        prioridad = _prioridad_del_cargo(buscar(fila, "CARGO"))
        if prioridad is None:
            continue

        contacto = _Contacto(
            responsable=normalizar_texto(buscar(fila, "NOMBRE", "RESPONSABLE")),
            anexo=normalizar_texto(buscar(fila, "ANEXO", "TELEFONO")),
            correo=normalizar_texto(buscar(fila, "CORREO", "EMAIL", "MAIL")),
        )

        llave = clave(unidad)
        anterior = mejores.get(llave)
        if anterior is None or prioridad < anterior[0]:
            mejores[llave] = (prioridad, contacto)

    return {llave: contacto for llave, (_, contacto) in mejores.items()}


def _prioridad_del_cargo(cargo: object) -> int | None:
    """Cuanto pesa un cargo para ser el contacto de la unidad.

    Menor numero, mas prioridad. Devuelve None cuando el cargo no es una
    jefatura: esa persona trabaja en la unidad, pero no es a quien corresponde
    dirigirse por el equipamiento.
    """
    texto = clave(cargo)
    if not texto:
        return None
    for posicion, palabra in enumerate(_CARGOS_DE_JEFATURA):
        if palabra in texto:
            return posicion
    return None
