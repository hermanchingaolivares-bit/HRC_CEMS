"""Pruebas de las reglas de limpieza.

Es donde el prototipo se equivocaba en silencio, asi que cada caso de aca
corresponde a un dato real de las planillas o a un defecto conocido.
"""

from datetime import date, datetime
from decimal import Decimal

import pytest

from etl.transform.normalizar import (
    FechaInvalida,
    ValorInvalido,
    convertir_entero,
    convertir_fecha,
    convertir_frecuencia,
    convertir_monto,
    dividir_series,
    es_nic_valido,
    esta_vacio,
    normalizar_clave,
    normalizar_serie,
    normalizar_texto,
)

# ----------------------------------------------------------------------
# Celdas vacias
# ----------------------------------------------------------------------


@pytest.mark.parametrize("valor", [None, "", "   ", "-", "N/A", "nan", "SIN INFORMACION"])
def test_esta_vacio_reconoce_los_rellenos_de_la_planilla(valor):
    assert esta_vacio(valor) is True


@pytest.mark.parametrize("valor", ["0", "A", "  X  "])
def test_esta_vacio_no_confunde_un_dato_real_con_vacio(valor):
    assert esta_vacio(valor) is False


# ----------------------------------------------------------------------
# Series
# ----------------------------------------------------------------------


def test_normalizar_serie_recorta_y_pasa_a_mayusculas():
    assert normalizar_serie("  j56004307 ") == "J56004307"


def test_normalizar_serie_conserva_los_espacios_interiores():
    # Son parte de la serie impresa por el fabricante, no ruido.
    assert normalizar_serie(" AB 123 ") == "AB 123"


def test_normalizar_serie_devuelve_none_si_la_celda_esta_vacia():
    assert normalizar_serie("   ") is None


@pytest.mark.parametrize(
    ("celda", "esperado"),
    [
        ("A123 B456", ["A123", "B456"]),
        ("A123:B456", ["A123", "B456"]),
        ("A123/B456", ["A123", "B456"]),
        ("A123//B456", ["A123", "B456"]),
        ("A123.", ["A123"]),
        ("", []),
    ],
)
def test_dividir_series_separa_una_celda_con_varias_series(celda, esperado):
    assert dividir_series(celda) == esperado


def test_dividir_series_aplica_todos_los_separadores_a_la_vez():
    # El prototipo encadenaba elif y dejaba "B/C" pegado.
    assert dividir_series("A:B/C") == ["A", "B", "C"]


def test_dividir_series_elimina_repetidas_conservando_el_orden():
    assert dividir_series("B123 A456 b123") == ["B123", "A456"]


# ----------------------------------------------------------------------
# Claves y NIC
# ----------------------------------------------------------------------


def test_normalizar_clave_quita_todos_los_espacios():
    # Con esto se casa el NIC del indice con el nombre de la hoja del Excel.
    assert normalizar_clave(" cdt-r23 24 ") == "CDT-R2324"


@pytest.mark.parametrize("nic", ["CDT-R02-07", "NEO-029-12", "IM001-23"])
def test_es_nic_valido_acepta_los_nic_reales(nic):
    assert es_nic_valido(nic) is True


@pytest.mark.parametrize("nic", ["", "0", ",", "APA-002-03", "ASSET 44", "COD123", None])
def test_es_nic_valido_rechaza_lo_que_no_es_un_nic(nic):
    assert es_nic_valido(nic) is False


def test_normalizar_texto_colapsa_espacios_repetidos():
    assert normalizar_texto("  cambio   de  bateria ") == "cambio de bateria"


# ----------------------------------------------------------------------
# Fechas
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [
        ("12/08/2026", date(2026, 8, 12)),
        ("12-08-2026", date(2026, 8, 12)),
        ("2026-08-12", date(2026, 8, 12)),
        ("2026/08/12", date(2026, 8, 12)),
        ("12/08/26", date(2026, 8, 12)),
        ("20260812", date(2026, 8, 12)),
        ("2024-08-12 00:00:00", date(2024, 8, 12)),
    ],
)
def test_convertir_fecha_acepta_los_formatos_de_las_planillas(valor, esperado):
    assert convertir_fecha(valor) == esperado


def test_convertir_fecha_usa_el_criterio_chileno_dia_primero():
    assert convertir_fecha("03/04/2026") == date(2026, 4, 3)


def test_convertir_fecha_acepta_lo_que_entrega_openpyxl():
    assert convertir_fecha(datetime(2025, 6, 25, 0, 0)) == date(2025, 6, 25)


def test_convertir_fecha_traduce_el_numero_de_serie_de_excel():
    assert convertir_fecha(45000) == date(2023, 3, 15)


def test_convertir_fecha_devuelve_none_cuando_la_celda_esta_vacia():
    assert convertir_fecha("") is None


@pytest.mark.parametrize("valor", ["12 de enero", "sin fecha", "??", "13/13/2026"])
def test_convertir_fecha_avisa_en_vez_de_inventar(valor):
    # El prototipo borraba las letras y convertia "12 de enero" en una fecha
    # inventada. Aca tiene que fallar para poder reportarlo como FECHA_INVALIDA.
    with pytest.raises(FechaInvalida):
        convertir_fecha(valor)


# ----------------------------------------------------------------------
# Numeros
# ----------------------------------------------------------------------


@pytest.mark.parametrize(("valor", "esperado"), [("10", 10), (10, 10), (10.0, 10), ("", None)])
def test_convertir_entero(valor, esperado):
    assert convertir_entero(valor) == esperado


@pytest.mark.parametrize("valor", ["diez", "10,5", 10.5])
def test_convertir_entero_avisa_si_no_es_entero(valor):
    with pytest.raises(ValorInvalido):
        convertir_entero(valor)


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [
        ("$ 1.234.567", Decimal("1234567")),
        ("1.234.567,89", Decimal("1234567.89")),
        ("16195403.56", Decimal("16195403.56")),
        ("1234567", Decimal("1234567")),
        ("", None),
    ],
)
def test_convertir_monto_entiende_como_escribe_montos_la_unidad(valor, esperado):
    assert convertir_monto(valor) == esperado


def test_convertir_monto_avisa_si_no_es_un_monto():
    with pytest.raises(ValorInvalido):
        convertir_monto("no cotizado")


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [("1", Decimal("1")), ("2", Decimal("2")), ("1/2", Decimal("0.5")), ("", None)],
)
def test_convertir_frecuencia(valor, esperado):
    assert convertir_frecuencia(valor) == esperado


@pytest.mark.parametrize("valor", ["anual", "0", "-1"])
def test_convertir_frecuencia_avisa_si_no_es_una_frecuencia(valor):
    with pytest.raises(ValorInvalido):
        convertir_frecuencia(valor)
