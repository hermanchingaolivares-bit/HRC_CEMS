"""Reglas de limpieza que comparten todas las fuentes.

Aca se concentran los errores que corrompen datos en silencio, asi que es lo
unico del ETL que se escribe con pruebas antes que su codigo usuario.

Diferencia con el prototipo: cuando un valor no se puede convertir, estas
funciones **avisan** levantando `ValorInvalido`. El prototipo devolvia un
valor vacio, y esa fila desaparecia sin que nadie se enterara. Quien llama
decide que hacer con el aviso; normalmente, anotarlo en la tabla `rechazo`.
"""

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

# Separadores que la unidad usa dentro de una misma celda para escribir
# varias series: "A123 B456", "A123:B456", "A123/B456", "A123//B456".
_SEPARADORES_DE_SERIE = re.compile(r"[\s:/]+")

_ESPACIOS_REPETIDOS = re.compile(r"\s+")

# Textos que las planillas usan para decir "aca no hay nada".
_VACIOS = {"", "-", "--", "NAN", "NONE", "NULL", "S/I", "SIN INFORMACION", "N/A", "NA"}

# Formatos de fecha vistos en las planillas, en orden de preferencia.
# El chileno va primero: 03/04/2026 es 3 de abril, no 4 de marzo.
_FORMATOS_DE_FECHA = (
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%y",
    "%d-%m-%y",
    "%Y%m%d",
)

# Prefijos y textos que no son un NIC aunque esten escritos en su columna.
_BLOQUEOS_DE_NIC = ("APA-", "AP-", "ASSET", "COD")


class ValorInvalido(ValueError):
    """Un valor no se pudo convertir y hay que reportarlo, no descartarlo."""

    def __init__(self, valor: object, esperado: str) -> None:
        super().__init__(f"No se pudo interpretar {valor!r} como {esperado}")
        self.valor = valor
        self.esperado = esperado


class FechaInvalida(ValorInvalido):
    """Caso particular de `ValorInvalido`, para el motivo FECHA_INVALIDA."""

    def __init__(self, valor: object) -> None:
        super().__init__(valor, "fecha")


# ----------------------------------------------------------------------
# Texto
# ----------------------------------------------------------------------


def esta_vacio(valor: object) -> bool:
    """True si la celda no dice nada, incluidos los rellenos tipo 'N/A'."""
    if valor is None:
        return True
    return str(valor).strip().upper() in _VACIOS


def normalizar_texto(valor: object) -> str | None:
    """Recorta y colapsa espacios. Devuelve None si la celda esta vacia."""
    if esta_vacio(valor):
        return None
    return _ESPACIOS_REPETIDOS.sub(" ", str(valor).strip())


def normalizar_serie(valor: object) -> str | None:
    """Normaliza una serie: recorte y mayusculas, nada mas.

    No se quitan los espacios interiores: forman parte de la serie que
    imprimio el fabricante. Para emparejar NIC con nombres de hoja, que si
    necesita quitarlos, esta `normalizar_clave`.
    """
    if esta_vacio(valor):
        return None
    return str(valor).strip().upper()


def dividir_series(valor: object) -> list[str]:
    """Separa una celda que contiene varias series en una lista.

    Aplica todos los separadores a la vez. El prototipo encadenaba `elif`,
    asi que "A:B/C" quedaba como "A" y "B/C" pegados.
    Se conserva el orden de aparicion y se eliminan las repetidas.
    """
    if esta_vacio(valor):
        return []

    series: list[str] = []
    for parte in _SEPARADORES_DE_SERIE.split(str(valor)):
        limpia = parte.strip().rstrip(".").strip().upper()
        if limpia and limpia not in _VACIOS and limpia not in series:
            series.append(limpia)
    return series


def normalizar_clave(valor: object) -> str:
    """Clave de emparejamiento: mayusculas y sin espacios de ningun tipo.

    Se usa para casar el NIC del indice con el nombre de la hoja del Excel,
    que puede venir escrito con espacios de mas.
    """
    if valor is None:
        return ""
    return _ESPACIOS_REPETIDOS.sub("", str(valor).strip().upper())


def es_nic_valido(valor: object) -> bool:
    """False cuando la columna NIC trae algo que no es un NIC.

    Los bloqueos vienen del prototipo: son conocimiento de la unidad que no
    se deduce mirando los datos.
    """
    if esta_vacio(valor):
        return False
    texto = str(valor).strip().upper()
    if texto in {"0", ","}:
        return False
    return all(bloqueo not in texto for bloqueo in _BLOQUEOS_DE_NIC)


# ----------------------------------------------------------------------
# Fechas
# ----------------------------------------------------------------------


def convertir_fecha(valor: object) -> date | None:
    """Convierte una fecha de planilla a `date`.

    Devuelve None solo si la celda esta vacia. Si tiene contenido y no se
    puede interpretar, levanta `FechaInvalida` para que quede reportada.

    El prototipo borraba con una expresion regular todo lo que no fuera
    numero o barra antes de convertir, asi que "12 de enero" se transformaba
    en "12  1" y terminaba siendo una fecha inventada. Aca no se adivina.
    """
    if esta_vacio(valor):
        return None

    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor

    # Excel guarda las fechas como dias contados desde el 30-12-1899.
    if isinstance(valor, int | float) and not isinstance(valor, bool):
        return _fecha_desde_serie_excel(valor)

    texto = str(valor).strip()

    # Las marcas de tiempo traen la hora pegada: se descarta, no se usa.
    texto_sin_hora = texto.split(" ")[0] if " " in texto else texto

    for formato in _FORMATOS_DE_FECHA:
        for candidato in (texto, texto_sin_hora):
            try:
                return datetime.strptime(candidato, formato).date()
            except ValueError:
                continue

    raise FechaInvalida(valor)


def _fecha_desde_serie_excel(numero: float) -> date:
    """Traduce el numero de dias con que Excel guarda una fecha."""
    if not 1 <= numero <= 400_000:
        raise FechaInvalida(numero)
    origen = date(1899, 12, 30)
    return origen.fromordinal(origen.toordinal() + int(numero))


# ----------------------------------------------------------------------
# Numeros
# ----------------------------------------------------------------------


def convertir_entero(valor: object) -> int | None:
    """Convierte a entero. None si esta vacio; avisa si trae basura."""
    if esta_vacio(valor):
        return None
    if isinstance(valor, bool):
        raise ValorInvalido(valor, "numero entero")
    if isinstance(valor, int):
        return valor
    if isinstance(valor, float):
        if valor != int(valor):
            raise ValorInvalido(valor, "numero entero")
        return int(valor)

    texto = str(valor).strip().replace(" ", "")
    try:
        numero = Decimal(texto)
    except InvalidOperation as error:
        raise ValorInvalido(valor, "numero entero") from error
    if numero != numero.to_integral_value():
        raise ValorInvalido(valor, "numero entero")
    return int(numero)


def convertir_monto(valor: object) -> Decimal | None:
    """Convierte un monto en pesos a `Decimal`.

    Las planillas escriben los montos de varias formas: "$ 1.234.567",
    "1.234.567,89" o "1234567.89". La regla es la chilena: el punto separa
    miles y la coma separa decimales. Un punto suelto seguido de tres
    digitos tambien se toma como separador de miles, que es como se escribe
    aca un monto sin centavos.
    """
    if esta_vacio(valor):
        return None
    if isinstance(valor, bool):
        raise ValorInvalido(valor, "monto")
    if isinstance(valor, int | float):
        return Decimal(str(valor))

    texto = str(valor).strip()
    for sobrante in ("$", "CLP", " ", "\xa0"):
        texto = texto.replace(sobrante, "")

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"-?\d{1,3}(\.\d{3})+", texto):
        texto = texto.replace(".", "")

    try:
        return Decimal(texto)
    except InvalidOperation as error:
        raise ValorInvalido(valor, "monto") from error


def convertir_frecuencia(valor: object) -> Decimal | None:
    """Convierte la frecuencia anual de mantenimiento.

    La planilla escribe cuantos mantenimientos hay en un anio: 1 anual,
    2 semestral, 3, 4. El caso especial es "1/2", que significa un
    mantenimiento cada dos anios y se guarda como 0,5.
    """
    if esta_vacio(valor):
        return None

    texto = str(valor).strip().replace(" ", "").replace(",", ".")
    if texto in {"1/2", "0.5", ".5"}:
        return Decimal("0.5")

    try:
        frecuencia = Decimal(texto)
    except InvalidOperation as error:
        raise ValorInvalido(valor, "frecuencia anual") from error

    if frecuencia <= 0:
        raise ValorInvalido(valor, "frecuencia anual")
    return frecuencia
