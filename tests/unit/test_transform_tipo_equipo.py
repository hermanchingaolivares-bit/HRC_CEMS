"""Pruebas de la transformacion de INDICES Y COSTOS a `tipo_equipo`.

Los encabezados y los valores de los ejemplos son los de la planilla real.
"""

import pytest

from etl.transform.columnas import ColumnaFaltante
from etl.transform.resultado import NOMBRE_DUPLICADO, VALOR_INVALIDO
from etl.transform.tipo_equipo import transformar


def fila(equipo="Monitor Desfibrilador", funcion="10", mantenimiento="5", riesgo="5",
         antecedentes="2", im="22"):
    """Arma una fila con los encabezados tal como vienen de la planilla."""
    return {
        "EQUIPO": equipo,
        "FUNCIÓN": funcion,
        "MANTENIMIENTO": mantenimiento,
        "RIESGO FÍSICO": riesgo,
        "ANTECEDENTES": antecedentes,
        "IM": im,
        "PROGRAMA": "SI",
        "ESTADO 2026": "CONVENIO",
    }


# ----------------------------------------------------------------------
# La categoria sale del indice
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("indice", "categoria"),
    [
        ("22", "CRITICO"),
        ("19", "RELEVANTE"),
        ("12", "IM_MAYOR_12"),
        ("15", "IM_MAYOR_12"),
        ("18", "IM_MAYOR_12"),
    ],
)
def test_la_categoria_se_deduce_del_indice(indice, categoria):
    resultado = transformar([fila(im=indice)])
    assert resultado.cargadas == 1
    assert resultado.filas[0].categoria == categoria


@pytest.mark.parametrize("indice", ["0", "5", "11"])
def test_los_tipos_bajo_doce_no_entran_y_no_son_un_defecto(indice):
    # No tienen plan de mantenimiento, asi que no pertenecen al sistema.
    resultado = transformar([fila(im=indice)])
    assert resultado.cargadas == 0
    assert resultado.rechazadas == 0
    assert resultado.ignoradas == 1


def test_un_tipo_sin_evaluar_no_entra_y_no_es_un_defecto():
    resultado = transformar([fila(im="", funcion="", mantenimiento="", riesgo="", antecedentes="")])
    assert resultado.cargadas == 0
    assert resultado.rechazadas == 0
    assert resultado.ignoradas == 1


@pytest.mark.parametrize("indice", ["20", "21", "25"])
def test_un_indice_fuera_de_la_escala_se_reporta(indice):
    resultado = transformar([fila(im=indice)])
    assert resultado.cargadas == 0
    assert resultado.rechazadas == 1
    assert resultado.rechazos[0].motivo == VALOR_INVALIDO


def test_un_indice_que_no_es_numero_se_reporta():
    resultado = transformar([fila(im="alto")])
    assert resultado.cargadas == 0
    assert resultado.rechazos[0].motivo == VALOR_INVALIDO
    assert "no es un numero" in resultado.rechazos[0].detalle


# ----------------------------------------------------------------------
# Los cuatro factores
# ----------------------------------------------------------------------


def test_los_factores_se_leen_con_tilde_o_sin_ella():
    sin_tildes = {
        "EQUIPO": "Bomba de infusion",
        "FUNCION": "7",
        "MANTENIMIENTO": "5",
        "RIESGO FISICO": "5",
        "ANTECEDENTES": "2",
        "IM": "19",
    }
    resultado = transformar([sin_tildes])
    tipo = resultado.filas[0]
    assert (tipo.im_funcion, tipo.im_mantenimiento) == (7, 5)
    assert (tipo.im_riesgo_fisico, tipo.im_antecedentes) == (5, 2)


def test_un_factor_ilegible_no_impide_cargar_el_tipo_pero_se_reporta():
    resultado = transformar([fila(funcion="diez")])
    assert resultado.cargadas == 1
    assert resultado.filas[0].im_funcion is None
    assert resultado.filas[0].categoria == "CRITICO"
    assert resultado.rechazos[0].motivo == VALOR_INVALIDO


def test_los_factores_vacios_quedan_nulos():
    resultado = transformar([fila(funcion="", mantenimiento="", riesgo="", antecedentes="")])
    tipo = resultado.filas[0]
    assert tipo.im_funcion is None
    assert tipo.im_antecedentes is None


# ----------------------------------------------------------------------
# Nombres
# ----------------------------------------------------------------------


def test_un_nombre_repetido_se_carga_una_vez_y_se_reporta():
    # Es el caso real de 'Mesa Qx Avanzada', que aparece dos veces en la hoja.
    resultado = transformar([fila(equipo="Mesa Qx Avanzada"), fila(equipo="Mesa Qx Avanzada")])
    assert resultado.cargadas == 1
    assert resultado.rechazadas == 1
    assert resultado.rechazos[0].motivo == NOMBRE_DUPLICADO


def test_la_repeticion_se_detecta_aunque_cambien_las_mayusculas():
    resultado = transformar([fila(equipo="Mesa Qx Avanzada"), fila(equipo="MESA QX AVANZADA")])
    assert resultado.cargadas == 1
    assert resultado.rechazos[0].motivo == NOMBRE_DUPLICADO


def test_el_nombre_se_recorta_y_se_conserva_como_lo_escribio_la_unidad():
    resultado = transformar([fila(equipo="  Monitor   Multiparametro  ")])
    assert resultado.filas[0].nombre == "Monitor Multiparametro"


def test_una_fila_sin_nombre_se_ignora():
    resultado = transformar([fila(equipo="")])
    assert resultado.cargadas == 0
    assert resultado.rechazadas == 0
    assert resultado.ignoradas == 1


# ----------------------------------------------------------------------
# La hoja completa
# ----------------------------------------------------------------------


def test_los_contadores_cuadran():
    filas = [
        fila(equipo="Incubadora", im="22"),
        fila(equipo="Analizador", im="19"),
        fila(equipo="Sillon dental", im="14"),
        fila(equipo="Balanza", im="8"),
        fila(equipo="Camilla", im=""),
        fila(equipo="Incubadora", im="22"),
        fila(equipo=""),
    ]
    resultado = transformar(filas)
    assert resultado.leidas == 7
    assert resultado.cargadas == 3
    assert resultado.rechazadas == 1
    assert resultado.ignoradas == 3


def test_una_hoja_vacia_no_falla():
    resultado = transformar([])
    assert resultado.leidas == 0
    assert resultado.cargadas == 0


def test_si_falta_una_columna_imprescindible_se_corta():
    with pytest.raises(ColumnaFaltante):
        transformar([{"NOMBRE DEL EQUIPO": "Monitor", "INDICE": "22"}])
