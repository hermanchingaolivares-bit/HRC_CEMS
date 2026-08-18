"""Registro de cada corrida del ETL: las tablas `carga`, `rechazo` y `cambio`.

Tres cosas que el esquema de la fase 1 espera y que el ETL tiene que cumplir:

1. Cada corrida abre una fila en `carga` con cuantas filas leyo, cargo y
   rechazo.
2. Lo que no se puede cargar se anota en `rechazo` con su motivo. Es la lista
   de correcciones pendientes que la unidad tiene que hacer en su planilla.
3. Antes de escribir, la transaccion declara que el autor es el ETL. Sin eso,
   los disparadores anotan cada cambio como si lo hubiera hecho una persona.

Todo pasa dentro de una sola transaccion por fuente: si algo falla a la
mitad, no queda media planilla cargada.
"""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.engine import Connection

from core.bd import obtener_motor

# Los mismos motivos que acepta la restriccion de la tabla `rechazo`.
# Se validan tambien aca para fallar con un mensaje claro y no con un error
# del motor a mitad de la carga.
MOTIVOS_VALIDOS = frozenset(
    {
        "SERIE_VACIA",
        "SERIE_DUPLICADA",
        "SIN_CATASTRO",
        "FECHA_INVALIDA",
        "SIN_PLAN",
        "TIPO_DESCONOCIDO",
    }
)

_registro = logging.getLogger(__name__)


@dataclass
class _Rechazo:
    motivo: str
    valor: str | None
    detalle: str | None


@dataclass
class Carga:
    """Una corrida sobre una fuente. Lleva la cuenta y junta los rechazos."""

    id_carga: int
    fuente: str
    conexion: Connection
    filas_leidas: int = 0
    filas_cargadas: int = 0
    _rechazos: list[_Rechazo] = field(default_factory=list, repr=False)

    def contar_leidas(self, cantidad: int = 1) -> None:
        self.filas_leidas += cantidad

    def contar_cargadas(self, cantidad: int = 1) -> None:
        self.filas_cargadas += cantidad

    def rechazar(self, motivo: str, valor: object = None, detalle: str | None = None) -> None:
        """Anota una fila que no entra, con el motivo por el que no entra."""
        if motivo not in MOTIVOS_VALIDOS:
            raise ValueError(
                f"Motivo de rechazo desconocido: {motivo!r}. "
                f"Los validos son {sorted(MOTIVOS_VALIDOS)}"
            )
        texto = None if valor is None else str(valor)[:500]
        self._rechazos.append(_Rechazo(motivo=motivo, valor=texto, detalle=detalle))

    @property
    def filas_rechazadas(self) -> int:
        return len(self._rechazos)

    def resumen(self) -> str:
        return (
            f"{self.fuente}: {self.filas_leidas} leidas, "
            f"{self.filas_cargadas} cargadas, {self.filas_rechazadas} rechazadas"
        )


@contextmanager
def carga_en_curso(fuente: str) -> Iterator[Carga]:
    """Abre una corrida sobre una fuente y la cierra al terminar.

    Uso:

        with carga_en_curso("PMP") as carga:
            carga.contar_leidas(len(filas))
            ...
            carga.rechazar("SERIE_VACIA", valor=fila["SERIE"])

    Si el bloque termina bien, se guardan los rechazos, se actualizan los
    contadores y se confirma la transaccion. Si levanta una excepcion, se
    deshace todo: la corrida fallida no deja datos a medias en la base. El
    rastro de lo que paso queda en el archivo de registro.
    """
    with obtener_motor().begin() as conexion:
        id_carga = conexion.execute(
            text("INSERT INTO carga (fuente) VALUES (:fuente) RETURNING id_carga"),
            {"fuente": fuente},
        ).scalar_one()

        _declarar_autoria(conexion, id_carga)

        carga = Carga(id_carga=id_carga, fuente=fuente, conexion=conexion)
        _registro.info("Carga %s iniciada sobre %s", id_carga, fuente)

        yield carga

        _guardar_rechazos(carga)
        _cerrar_carga(carga)
        _registro.info("Carga %s terminada. %s", id_carga, carga.resumen())


def _declarar_autoria(conexion: Connection, id_carga: int) -> None:
    """Marca la transaccion como escrita por el ETL.

    Los disparadores leen estas dos variables para llenar `cambio.origen` y
    `cambio.carga_id`. Se usa `set_config` en vez de `SET LOCAL` porque
    permite pasar el valor como parametro. El tercer argumento en `true`
    hace que valga solo dentro de esta transaccion.
    """
    conexion.execute(text("SELECT set_config('hrc.origen', 'ETL', true)"))
    conexion.execute(
        text("SELECT set_config('hrc.carga_id', :id_carga, true)"),
        {"id_carga": str(id_carga)},
    )


def _guardar_rechazos(carga: Carga) -> None:
    if not carga._rechazos:
        return
    carga.conexion.execute(
        text(
            "INSERT INTO rechazo (carga_id, fuente, motivo, valor, detalle) "
            "VALUES (:carga_id, :fuente, :motivo, :valor, :detalle)"
        ),
        [
            {
                "carga_id": carga.id_carga,
                "fuente": carga.fuente,
                "motivo": rechazo.motivo,
                "valor": rechazo.valor,
                "detalle": rechazo.detalle,
            }
            for rechazo in carga._rechazos
        ],
    )


def _cerrar_carga(carga: Carga) -> None:
    carga.conexion.execute(
        text(
            "UPDATE carga SET filas_leidas = :leidas, filas_cargadas = :cargadas, "
            "filas_rechazadas = :rechazadas WHERE id_carga = :id_carga"
        ),
        {
            "leidas": carga.filas_leidas,
            "cargadas": carga.filas_cargadas,
            "rechazadas": carga.filas_rechazadas,
            "id_carga": carga.id_carga,
        },
    )
