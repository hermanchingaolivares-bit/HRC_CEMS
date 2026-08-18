"""Pruebas del registro de corridas contra la base de datos real.

Van marcadas como `integration` porque necesitan PostgreSQL corriendo. Para
ejecutarlas:

    C:\\Users\\herma\\anaconda3\\envs\\cems\\python.exe -m pytest -m integration -v

Cada prueba limpia lo que crea, asi que la base queda como estaba.
"""

import pytest
from sqlalchemy import text

from core.bd import obtener_motor
from etl.load.registro import carga_en_curso

pytestmark = pytest.mark.integration

SERIE_DE_PRUEBA = "PRUEBA-TEST-0001"
TIPO_DE_PRUEBA = "PRUEBA TEST Ventilador"


@pytest.fixture
def limpiar_rastros():
    """Borra lo que la prueba haya dejado, pase o falle."""
    yield
    with obtener_motor().begin() as conexion:
        conexion.execute(
            text(
                "DELETE FROM cambio WHERE carga_id IN "
                "(SELECT id_carga FROM carga WHERE fuente LIKE 'PRUEBA_TEST%')"
            )
        )
        conexion.execute(
            text("DELETE FROM equipo WHERE serie = :serie"), {"serie": SERIE_DE_PRUEBA}
        )
        conexion.execute(text("DELETE FROM tipo_equipo WHERE nombre = :n"), {"n": TIPO_DE_PRUEBA})
        conexion.execute(
            text(
                "DELETE FROM rechazo WHERE carga_id IN "
                "(SELECT id_carga FROM carga WHERE fuente LIKE 'PRUEBA_TEST%')"
            )
        )
        conexion.execute(text("DELETE FROM carga WHERE fuente LIKE 'PRUEBA_TEST%'"))


def test_una_corrida_deja_su_registro_y_sus_rechazos(limpiar_rastros):
    with carga_en_curso("PRUEBA_TEST_REGISTRO") as carga:
        id_carga = carga.id_carga
        carga.contar_leidas(3)
        carga.contar_cargadas(1)
        carga.rechazar("SERIE_VACIA", valor="", detalle="fila sin serie")
        carga.rechazar("SIN_PLAN", valor="ABC123")

    with obtener_motor().connect() as conexion:
        leidas, cargadas, rechazadas = conexion.execute(
            text(
                "SELECT filas_leidas, filas_cargadas, filas_rechazadas "
                "FROM carga WHERE id_carga = :id"
            ),
            {"id": id_carga},
        ).one()
        motivos = [
            fila[0]
            for fila in conexion.execute(
                text("SELECT motivo FROM rechazo WHERE carga_id = :id ORDER BY id_rechazo"),
                {"id": id_carga},
            )
        ]

    assert (leidas, cargadas, rechazadas) == (3, 1, 2)
    assert motivos == ["SERIE_VACIA", "SIN_PLAN"]


def test_los_disparadores_reconocen_que_escribio_el_etl(limpiar_rastros):
    """Sin la declaracion de autoria, el cambio quedaria anotado como MANUAL."""
    with carga_en_curso("PRUEBA_TEST_AUTORIA") as carga:
        id_carga = carga.id_carga
        id_tipo = carga.conexion.execute(
            text(
                "INSERT INTO tipo_equipo (nombre, categoria) VALUES (:n, 'CRITICO') "
                "RETURNING id_tipo_equipo"
            ),
            {"n": TIPO_DE_PRUEBA},
        ).scalar_one()
        carga.conexion.execute(
            text("INSERT INTO equipo (serie, tipo_equipo_id, marca) VALUES (:s, :t, 'Antes')"),
            {"s": SERIE_DE_PRUEBA, "t": id_tipo},
        )
        carga.conexion.execute(
            text("UPDATE equipo SET marca = 'Despues' WHERE serie = :s"), {"s": SERIE_DE_PRUEBA}
        )

    with obtener_motor().connect() as conexion:
        campo, anterior, nuevo, origen = conexion.execute(
            text(
                "SELECT campo, valor_anterior, valor_nuevo, origen "
                "FROM cambio WHERE carga_id = :id AND campo = 'marca'"
            ),
            {"id": id_carga},
        ).one()

    assert (campo, anterior, nuevo, origen) == ("marca", "Antes", "Despues", "ETL")


def test_una_corrida_que_falla_no_deja_datos_a_medias(limpiar_rastros):
    with pytest.raises(RuntimeError), carga_en_curso("PRUEBA_TEST_ROLLBACK") as carga:
        carga.conexion.execute(
            text("INSERT INTO tipo_equipo (nombre, categoria) VALUES (:n, 'CRITICO')"),
            {"n": TIPO_DE_PRUEBA},
        )
        raise RuntimeError("falla simulada a mitad de la carga")

    with obtener_motor().connect() as conexion:
        tipos = conexion.execute(
            text("SELECT count(*) FROM tipo_equipo WHERE nombre = :n"), {"n": TIPO_DE_PRUEBA}
        ).scalar_one()
        cargas = conexion.execute(
            text("SELECT count(*) FROM carga WHERE fuente = 'PRUEBA_TEST_ROLLBACK'")
        ).scalar_one()

    assert tipos == 0
    assert cargas == 0


def test_un_motivo_de_rechazo_inventado_se_rechaza_de_inmediato(limpiar_rastros):
    """Mejor fallar aca que con un error del motor a mitad de la carga."""
    with (
        pytest.raises(ValueError, match="Motivo de rechazo desconocido"),
        carga_en_curso("PRUEBA_TEST_MOTIVO") as carga,
    ):
        carga.rechazar("MOTIVO_QUE_NO_EXISTE", valor="x")
