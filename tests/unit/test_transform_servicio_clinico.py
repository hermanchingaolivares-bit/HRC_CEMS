"""Pruebas del cruce entre Datos_Unidades y Agenda para `servicio_clinico`."""

import pytest

from etl.transform.columnas import ColumnaFaltante
from etl.transform.resultado import NOMBRE_DUPLICADO
from etl.transform.servicio_clinico import transformar


def unidad(nombre):
    """Datos_Unidades es una sola columna."""
    return {"LISTADO DE UNIDADES": nombre}


def contacto(unidad_, cargo="Jefa", nombre="Rosario Irribarren", correo="rosario@redsalud.gob.cl",
             anexo="527041"):
    """Una fila de la Agenda, con sus encabezados reales."""
    return {
        "UNIDAD": unidad_,
        "CARGO": cargo,
        "NOMBRE": nombre,
        "CORREO": correo,
        "ANEXO": anexo,
        "OBSERVACIONES": "-",
    }


# ----------------------------------------------------------------------
# El listado manda
# ----------------------------------------------------------------------


def test_carga_las_unidades_del_listado():
    resultado = transformar([unidad("ADMISION"), unidad("ANATOMIA PATOLOGICA")], [])
    assert resultado.cargadas == 2
    assert [servicio.nombre for servicio in resultado.filas] == ["ADMISION", "ANATOMIA PATOLOGICA"]


def test_una_unidad_sin_contacto_se_carga_igual():
    # Perder la unidad seria peor que no tener su telefono.
    resultado = transformar([unidad("PABELLON")], [])
    servicio = resultado.filas[0]
    assert servicio.nombre == "PABELLON"
    assert servicio.responsable is None
    assert servicio.correo is None


def test_una_unidad_de_la_agenda_que_no_esta_en_el_listado_no_se_inventa():
    resultado = transformar([unidad("PABELLON")], [contacto("ABASTECIMIENTO")])
    assert resultado.cargadas == 1
    assert resultado.filas[0].nombre == "PABELLON"
    assert resultado.ignoradas == 1


def test_una_unidad_repetida_se_carga_una_vez_y_se_reporta():
    resultado = transformar([unidad("URGENCIA"), unidad("urgencia")], [])
    assert resultado.cargadas == 1
    assert resultado.rechazos[0].motivo == NOMBRE_DUPLICADO


def test_una_fila_vacia_del_listado_se_ignora():
    resultado = transformar([unidad(""), unidad("UCI")], [])
    assert resultado.cargadas == 1
    assert resultado.ignoradas == 1


# ----------------------------------------------------------------------
# El contacto sale de la agenda
# ----------------------------------------------------------------------


def test_toma_el_contacto_de_la_agenda():
    resultado = transformar([unidad("NEONATOLOGIA")], [contacto("NEONATOLOGIA")])
    servicio = resultado.filas[0]
    assert servicio.responsable == "Rosario Irribarren"
    assert servicio.correo == "rosario@redsalud.gob.cl"
    assert servicio.anexo == "527041"


def test_el_cruce_no_depende_de_tildes_ni_mayusculas():
    resultado = transformar([unidad("ANATOMÍA PATOLÓGICA")], [contacto("anatomia patologica")])
    assert resultado.filas[0].responsable == "Rosario Irribarren"


def test_entre_varias_personas_toma_la_que_tiene_jefatura():
    agenda = [
        contacto("UCI", cargo="Operador de Compras", nombre="Nancy Veliz"),
        contacto("UCI", cargo="Jefe", nombre="Juan Navarro"),
        contacto("UCI", cargo="Tecnico", nombre="Pedro Soto"),
    ]
    resultado = transformar([unidad("UCI")], agenda)
    assert resultado.filas[0].responsable == "Juan Navarro"


def test_la_jefatura_titular_le_gana_a_la_encargada():
    agenda = [
        contacto("UCI", cargo="Encargado", nombre="Ana Rojas"),
        contacto("UCI", cargo="Jefa (S)", nombre="Marta Diaz"),
    ]
    resultado = transformar([unidad("UCI")], agenda)
    assert resultado.filas[0].responsable == "Marta Diaz"


def test_si_nadie_tiene_jefatura_la_unidad_queda_sin_contacto():
    agenda = [contacto("UCI", cargo="Tecnico", nombre="Pedro Soto")]
    resultado = transformar([unidad("UCI")], agenda)
    servicio = resultado.filas[0]
    assert servicio.nombre == "UCI"
    assert servicio.responsable is None


def test_una_fila_de_agenda_sin_unidad_no_rompe_el_cruce():
    agenda = [contacto("", cargo="Jefe"), contacto("UCI", cargo="Jefe", nombre="Juan Navarro")]
    resultado = transformar([unidad("UCI")], agenda)
    assert resultado.filas[0].responsable == "Juan Navarro"


def test_si_la_agenda_se_leyo_con_el_encabezado_equivocado_se_corta():
    """Leerla desde la fila 1 deja el titulo como encabezado y las columnas sin nombre.

    Antes eso pasaba inadvertido y todas las unidades quedaban sin contacto.
    """
    agenda_mal_leida = [
        {
            "AGENDA AÑO 2026": "Abastecimiento",
            "columna_2": "Encargado",
            "columna_3": "Juan Navarro Sotelo",
            "columna_4": "juan.navarro@redsalud.gob.cl",
        }
    ]
    with pytest.raises(ColumnaFaltante):
        transformar([unidad("UCI")], agenda_mal_leida)


def test_los_contadores_cuadran():
    unidades = [unidad("UCI"), unidad("PABELLON"), unidad("uci"), unidad("")]
    agenda = [contacto("UCI"), contacto("BODEGA")]
    resultado = transformar(unidades, agenda)
    assert resultado.leidas == 4
    assert resultado.cargadas == 2
    assert resultado.rechazadas == 1
    assert resultado.ignoradas == 2
