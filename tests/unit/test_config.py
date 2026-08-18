"""Pruebas de la configuracion.

Lo que se comprueba aca es que el ETL se detenga con un mensaje entendible
cuando falta algo, en vez de arrancar y cargar cero filas en silencio.
"""

import pytest
from pydantic import ValidationError

from core.config import RAIZ_PROYECTO, Ajustes, ErrorDeConfiguracion

VARIABLES = ("GOOGLE_CREDENTIALS_PATH", "EXCEL_HOJA_DE_VIDA_PATH", "DATABASE_URL")


def _ajustes(**valores):
    """Construye la configuracion sin leer el .env de la maquina."""
    base = {
        "GOOGLE_CREDENTIALS_PATH": "secrets/credentials.json",
        "EXCEL_HOJA_DE_VIDA_PATH": "data/raw/excel/HOJAS_DE_VIDA.xlsm",
        "DATABASE_URL": "postgresql+psycopg://usuario:clave@localhost:5432/hrc_cems",
    }
    base.update(valores)
    return Ajustes(_env_file=None, **base)


def test_una_ruta_relativa_se_resuelve_desde_la_raiz_del_proyecto():
    ajustes = _ajustes()
    assert (
        ajustes.ruta_credenciales_google == (RAIZ_PROYECTO / "secrets/credentials.json").resolve()
    )


def test_una_ruta_absoluta_se_respeta_tal_cual(tmp_path):
    # Es el caso del servidor: la ruta apunta a la unidad de red.
    archivo = tmp_path / "HOJAS_DE_VIDA.xlsm"
    ajustes = _ajustes(EXCEL_HOJA_DE_VIDA_PATH=str(archivo))
    assert ajustes.ruta_excel_hoja_de_vida == archivo


def test_falla_si_falta_una_variable(monkeypatch):
    for variable in VARIABLES:
        monkeypatch.delenv(variable, raising=False)
    with pytest.raises(ValidationError):
        Ajustes(_env_file=None)


def test_avisa_cuando_el_excel_no_esta_donde_dice_el_env(tmp_path):
    ajustes = _ajustes(EXCEL_HOJA_DE_VIDA_PATH=str(tmp_path / "no_existe.xlsm"))
    with pytest.raises(ErrorDeConfiguracion) as error:
        ajustes.exigir_excel_hoja_de_vida()
    # El mensaje tiene que decir que variable revisar y recordar que cambia
    # de un computador a otro.
    assert "EXCEL_HOJA_DE_VIDA_PATH" in str(error.value)
    assert "distinta en cada computador" in str(error.value)


def test_avisa_cuando_faltan_las_credenciales_de_google(tmp_path):
    ajustes = _ajustes(GOOGLE_CREDENTIALS_PATH=str(tmp_path / "credentials.json"))
    with pytest.raises(ErrorDeConfiguracion) as error:
        ajustes.exigir_credenciales_google()
    assert "GOOGLE_CREDENTIALS_PATH" in str(error.value)


def test_devuelve_la_ruta_cuando_el_archivo_si_existe(tmp_path):
    archivo = tmp_path / "HOJAS_DE_VIDA.xlsm"
    archivo.write_bytes(b"")
    ajustes = _ajustes(EXCEL_HOJA_DE_VIDA_PATH=str(archivo))
    assert ajustes.exigir_excel_hoja_de_vida() == archivo
