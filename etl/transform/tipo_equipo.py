"""De la hoja INDICES Y COSTOS al catalogo `tipo_equipo`.

La hoja tiene una fila por tipo de equipo -- no por equipo fisico -- con los
cuatro factores del indice de Fennigkoh y Smith y el indice total.

La categoria **no esta escrita en la hoja**: se deduce del indice, siguiendo
la regla del glosario. A criticos y relevantes la unidad les asigno 22 y 19
para poder reunirlos con los demas en un solo indice.

    22        ->  CRITICO
    19        ->  RELEVANTE
    12 a 18   ->  IM_MAYOR_12

Los tipos con indice bajo 12 y los que estan sin evaluar **no entran**: no
tienen plan de mantenimiento, y la categoria es obligatoria en la base. No son
un defecto de dato, asi que se cuentan como ignorados y no se reportan.
"""

import logging
from dataclasses import dataclass

from etl.transform.columnas import buscar, exigir_columnas
from etl.transform.normalizar import (
    ValorInvalido,
    convertir_entero,
    esta_vacio,
    normalizar_texto,
)
from etl.transform.resultado import NOMBRE_DUPLICADO, VALOR_INVALIDO, Resultado

FUENTE = "INDICES Y COSTOS"

INDICE_CRITICO = 22
INDICE_RELEVANTE = 19
INDICE_MINIMO_CON_PLAN = 12
INDICE_MAXIMO_IM = 18

_registro = logging.getLogger(__name__)


@dataclass(frozen=True)
class TipoEquipo:
    """Una fila lista para `tipo_equipo`."""

    nombre: str
    categoria: str
    im_funcion: int | None
    im_mantenimiento: int | None
    im_riesgo_fisico: int | None
    im_antecedentes: int | None


def transformar(filas: list[dict[str, str]]) -> Resultado[TipoEquipo]:
    """Convierte la hoja en tipos de equipo listos para cargar."""
    resultado: Resultado[TipoEquipo] = Resultado(leidas=len(filas))
    if not filas:
        return resultado

    exigir_columnas(filas[0], "EQUIPO", "IM")

    vistos: dict[str, str] = {}

    for fila in filas:
        nombre = normalizar_texto(buscar(fila, "EQUIPO"))
        if nombre is None:
            resultado.ignorar()
            continue

        indice_bruto = buscar(fila, "IM")
        if esta_vacio(indice_bruto):
            # Tipo sin evaluar: no tiene indice, luego no tiene plan.
            resultado.ignorar()
            continue

        try:
            indice = convertir_entero(indice_bruto)
        except ValorInvalido:
            resultado.rechazar(
                VALOR_INVALIDO,
                valor=indice_bruto,
                detalle=f"El indice de mantenimiento de '{nombre}' no es un numero",
            )
            continue

        if indice is None or indice < INDICE_MINIMO_CON_PLAN:
            # Bajo 12 no hay plan de mantenimiento: el tipo no entra al sistema.
            resultado.ignorar()
            continue

        categoria = _categoria_desde_indice(indice)
        if categoria is None:
            resultado.rechazar(
                VALOR_INVALIDO,
                valor=indice_bruto,
                detalle=(
                    f"El indice {indice} de '{nombre}' no corresponde a ninguna categoria: "
                    f"se esperaba 12 a 18, 19 o 22"
                ),
            )
            continue

        clave = nombre.upper()
        if clave in vistos:
            resultado.rechazar(
                NOMBRE_DUPLICADO,
                valor=nombre,
                detalle=f"El tipo ya aparecio antes como '{vistos[clave]}'. Se carga una sola vez",
            )
            continue
        vistos[clave] = nombre

        factores = _leer_factores(fila, nombre, resultado)
        resultado.aceptar(
            TipoEquipo(
                nombre=nombre,
                categoria=categoria,
                im_funcion=factores[0],
                im_mantenimiento=factores[1],
                im_riesgo_fisico=factores[2],
                im_antecedentes=factores[3],
            )
        )
        _avisar_si_los_factores_no_suman(nombre, factores, indice)

    _registro.info("%s: %s", FUENTE, resultado.resumen())
    return resultado


def _categoria_desde_indice(indice: int) -> str | None:
    if indice == INDICE_CRITICO:
        return "CRITICO"
    if indice == INDICE_RELEVANTE:
        return "RELEVANTE"
    if INDICE_MINIMO_CON_PLAN <= indice <= INDICE_MAXIMO_IM:
        return "IM_MAYOR_12"
    return None


def _leer_factores(
    fila: dict[str, str], nombre: str, resultado: Resultado[TipoEquipo]
) -> tuple[int | None, int | None, int | None, int | None]:
    """Los cuatro factores del indice. Si alguno no se entiende, queda vacio.

    Un factor ilegible no impide cargar el tipo: la categoria ya se decidio con
    el indice. Se anota para que la unidad lo corrija.
    """
    factores: list[int | None] = []
    for columna, alternativas in (
        ("FUNCION", ("FUNCION", "FUNCIÓN")),
        ("MANTENIMIENTO", ("MANTENIMIENTO",)),
        ("RIESGO FISICO", ("RIESGO FISICO", "RIESGO FÍSICO")),
        ("ANTECEDENTES", ("ANTECEDENTES",)),
    ):
        bruto = buscar(fila, *alternativas)
        try:
            factores.append(convertir_entero(bruto))
        except ValorInvalido:
            resultado.rechazar(
                VALOR_INVALIDO,
                valor=bruto,
                detalle=f"El factor {columna} de '{nombre}' no es un numero",
            )
            factores.append(None)
    return (factores[0], factores[1], factores[2], factores[3])


def _avisar_si_los_factores_no_suman(
    nombre: str, factores: tuple[int | None, ...], indice: int
) -> None:
    """La base recalcula el indice sumando los factores.

    Si la hoja dice un indice y sus factores suman otro, la base va a mostrar
    un numero distinto al de la planilla. No se rechaza -- la categoria ya se
    decidio con el indice de la hoja -- pero queda avisado en el registro.
    """
    if any(factor is None for factor in factores):
        return
    suma = sum(factor for factor in factores if factor is not None)
    if suma != indice:
        _registro.warning(
            "'%s': la hoja dice indice %s pero sus factores suman %s", nombre, indice, suma
        )
