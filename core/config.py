"""Configuracion del proyecto: lee el archivo .env y valida lo que hace falta.

Este es el unico modulo que toca el entorno. El resto del codigo pide los
valores aca y nunca lee variables de entorno por su cuenta.

La razon importa: `EXCEL_HOJA_DE_VIDA_PATH` **cambia segun el computador**.
En el PC de desarrollo apunta a una copia local; en el PC servidor del
hospital, al archivo original de la unidad de red que la unidad edita a
diario. Si esa ruta estuviera escrita en el codigo, mover el sistema al
hospital significaria editar codigo en vez de editar una linea del .env.
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from core.rutas import RAIZ_PROYECTO

ARCHIVO_ENV = RAIZ_PROYECTO / ".env"


class ErrorDeConfiguracion(RuntimeError):
    """Falta una variable del .env o apunta a algo que no existe."""


class Ajustes(BaseSettings):
    """Valores del .env, ya convertidos y con nombres del dominio."""

    model_config = SettingsConfigDict(
        env_file=ARCHIVO_ENV,
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    ruta_credenciales_google: Path = Field(alias="GOOGLE_CREDENTIALS_PATH")
    ruta_excel_hoja_de_vida: Path = Field(alias="EXCEL_HOJA_DE_VIDA_PATH")
    url_base_datos: str = Field(alias="DATABASE_URL")

    nombre_libro_google: str = Field(default="EEMM", alias="GOOGLE_SHEET_NOMBRE")
    nivel_bitacora: str = Field(default="INFO", alias="LOG_LEVEL")

    @field_validator("ruta_credenciales_google", "ruta_excel_hoja_de_vida")
    @classmethod
    def _resolver_ruta(cls, valor: Path) -> Path:
        """Una ruta relativa se interpreta desde la raiz del proyecto."""
        return valor if valor.is_absolute() else (RAIZ_PROYECTO / valor).resolve()

    # ------------------------------------------------------------------
    # Comprobaciones que se hacen antes de correr, no a mitad de camino
    # ------------------------------------------------------------------

    def exigir_credenciales_google(self) -> Path:
        """Devuelve la ruta del archivo de credenciales, o falla explicando."""
        if not self.ruta_credenciales_google.is_file():
            raise ErrorDeConfiguracion(
                f"No se encontro el archivo de credenciales de Google en "
                f"{self.ruta_credenciales_google}.\n"
                f"Revisa GOOGLE_CREDENTIALS_PATH en {ARCHIVO_ENV}. "
                f"El archivo no se versiona: hay que copiarlo a mano en cada maquina."
            )
        return self.ruta_credenciales_google

    def exigir_excel_hoja_de_vida(self) -> Path:
        """Devuelve la ruta del .xlsm, o falla explicando cual es el problema."""
        if not self.ruta_excel_hoja_de_vida.is_file():
            raise ErrorDeConfiguracion(
                f"No se encontro el Excel de hojas de vida en "
                f"{self.ruta_excel_hoja_de_vida}.\n"
                f"Revisa EXCEL_HOJA_DE_VIDA_PATH en {ARCHIVO_ENV}. "
                f"Recuerda que esa ruta es distinta en cada computador: en el servidor "
                f"apunta al archivo original de la unidad de red, no a una copia."
            )
        return self.ruta_excel_hoja_de_vida


@lru_cache(maxsize=1)
def obtener_ajustes() -> Ajustes:
    """Lee la configuracion una sola vez por proceso.

    Si falta una variable, traduce el error de pydantic a un mensaje que
    diga que variable falta y en que archivo se arregla.
    """
    try:
        return Ajustes()  # type: ignore[call-arg]  # los valores vienen del .env
    except ValidationError as error:
        faltantes = [str(detalle["loc"][0]) for detalle in error.errors()]
        raise ErrorDeConfiguracion(
            f"Falta configuracion en {ARCHIVO_ENV}: {', '.join(faltantes)}.\n"
            f"Copia .env.example como .env y completa los valores de esta maquina."
        ) from error
