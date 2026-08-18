"""Lo que devuelve una transformacion: lo que sirve y lo que no.

Toda transformacion de una planilla entrega dos cosas: las filas listas para
cargar y la lista de lo que no se pudo usar, con su motivo. Nunca descarta en
silencio, porque cada descarte es una correccion pendiente en la planilla de
la unidad.

Aparte se cuentan las filas **ignoradas**, que no son lo mismo que un rechazo:
una fila en blanco al final de la hoja, o un tipo de equipo sin plan de
mantenimiento, no son defectos que alguien deba corregir. Se cuentan para que
los numeros cuadren, pero no ensucian la tabla `rechazo`.
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")

# Motivos que usan las transformaciones de catalogos. Los de equipos y series
# ya venian del esquema de la fase 1; estos dos son nuevos.
NOMBRE_DUPLICADO = "NOMBRE_DUPLICADO"
VALOR_INVALIDO = "VALOR_INVALIDO"


@dataclass(frozen=True)
class FilaRechazada:
    """Una fila que no entra, y por que."""

    motivo: str
    valor: str | None = None
    detalle: str | None = None


@dataclass
class Resultado(Generic[T]):
    """Las filas buenas, los rechazos y los contadores de una transformacion."""

    filas: list[T] = field(default_factory=list)
    rechazos: list[FilaRechazada] = field(default_factory=list)
    leidas: int = 0
    ignoradas: int = 0

    def aceptar(self, fila: T) -> None:
        self.filas.append(fila)

    def rechazar(self, motivo: str, valor: object = None, detalle: str | None = None) -> None:
        texto = None if valor is None else str(valor)[:500]
        self.rechazos.append(FilaRechazada(motivo=motivo, valor=texto, detalle=detalle))

    def ignorar(self, cantidad: int = 1) -> None:
        self.ignoradas += cantidad

    @property
    def cargadas(self) -> int:
        return len(self.filas)

    @property
    def rechazadas(self) -> int:
        return len(self.rechazos)

    def resumen(self) -> str:
        return (
            f"{self.leidas} filas leidas, {self.cargadas} utilizables, "
            f"{self.rechazadas} rechazadas, {self.ignoradas} ignoradas"
        )
