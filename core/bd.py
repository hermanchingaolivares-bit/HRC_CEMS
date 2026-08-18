"""Conexion con PostgreSQL.

El esquema esta escrito a mano en SQL y es la fuente de la verdad, asi que
aca no hay modelos ORM: solo el motor de SQLAlchemy para ejecutar consultas.
"""

from functools import lru_cache

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from core.config import ErrorDeConfiguracion, obtener_ajustes


@lru_cache(maxsize=1)
def obtener_motor() -> Engine:
    """Crea el motor una sola vez por proceso."""
    ajustes = obtener_ajustes()
    return create_engine(ajustes.url_base_datos, future=True)


def verificar_conexion() -> None:
    """Comprueba que la base responde antes de empezar a trabajar.

    Vale la pena fallar aca, con un mensaje claro, y no a mitad de una carga
    con un error de driver que no dice nada.
    """
    try:
        with obtener_motor().connect() as conexion:
            conexion.execute(text("SELECT 1"))
    except SQLAlchemyError as error:
        raise ErrorDeConfiguracion(
            "No se pudo conectar con la base de datos.\n"
            "Revisa DATABASE_URL en el .env y que el servicio de PostgreSQL este corriendo.\n"
            f"Detalle: {error}"
        ) from error
