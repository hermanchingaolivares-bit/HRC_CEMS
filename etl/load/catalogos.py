"""Carga de los catalogos `tipo_equipo` y `servicio_clinico`.

Los catalogos se cargan desde las planillas como cualquier otra fuente: no hay
semillas escritas a mano que alguien tenga que mantener al dia.

La carga es **idempotente**: si el tipo o la unidad ya existen, se actualizan
sus datos en vez de duplicarse. Eso se apoya en que `nombre` es unico en las
dos tablas. Volver a correr la carga sobre una planilla sin cambios no deja
ninguna fila nueva ni ningun cambio registrado, porque el disparador de
`tipo_equipo` solo anota los campos que de verdad cambiaron de valor.
"""

import logging
from dataclasses import asdict

from sqlalchemy import text

from etl.load.registro import Carga
from etl.transform.servicio_clinico import ServicioClinico
from etl.transform.tipo_equipo import TipoEquipo

_registro = logging.getLogger(__name__)

_INSERTAR_TIPO_EQUIPO = text(
    """
    INSERT INTO tipo_equipo (
        nombre, categoria, im_funcion, im_mantenimiento, im_riesgo_fisico, im_antecedentes
    )
    VALUES (
        :nombre, :categoria, :im_funcion, :im_mantenimiento, :im_riesgo_fisico, :im_antecedentes
    )
    ON CONFLICT (nombre) DO UPDATE SET
        categoria        = EXCLUDED.categoria,
        im_funcion       = EXCLUDED.im_funcion,
        im_mantenimiento = EXCLUDED.im_mantenimiento,
        im_riesgo_fisico = EXCLUDED.im_riesgo_fisico,
        im_antecedentes  = EXCLUDED.im_antecedentes
    """
)

_INSERTAR_SERVICIO_CLINICO = text(
    """
    INSERT INTO servicio_clinico (nombre, responsable, anexo, correo)
    VALUES (:nombre, :responsable, :anexo, :correo)
    ON CONFLICT (nombre) DO UPDATE SET
        responsable = EXCLUDED.responsable,
        anexo       = EXCLUDED.anexo,
        correo      = EXCLUDED.correo
    """
)


def cargar_tipos_de_equipo(carga: Carga, tipos: list[TipoEquipo]) -> int:
    """Escribe los tipos de equipo. Devuelve cuantos se cargaron."""
    if not tipos:
        _registro.warning("No hay tipos de equipo que cargar")
        return 0

    carga.conexion.execute(_INSERTAR_TIPO_EQUIPO, [asdict(tipo) for tipo in tipos])
    _registro.info("tipo_equipo: %s filas escritas", len(tipos))
    return len(tipos)


def cargar_servicios_clinicos(carga: Carga, servicios: list[ServicioClinico]) -> int:
    """Escribe las unidades del hospital. Devuelve cuantas se cargaron."""
    if not servicios:
        _registro.warning("No hay servicios clinicos que cargar")
        return 0

    carga.conexion.execute(_INSERTAR_SERVICIO_CLINICO, [asdict(servicio) for servicio in servicios])
    _registro.info("servicio_clinico: %s filas escritas", len(servicios))
    return len(servicios)
