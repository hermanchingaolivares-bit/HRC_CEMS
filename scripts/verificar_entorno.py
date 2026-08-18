"""Comprueba que esta maquina puede correr el ETL. Ejecutar esto primero.

Para correrlo en Spyder: abrir este archivo y presionar F5. No hace falta
configurar nada mas ni pararse en ninguna carpeta en particular; el script se
ubica solo.

Revisa, en orden, las cuatro cosas que cambian de un computador a otro:

    1. El archivo .env y sus variables
    2. Las credenciales de Google
    3. El Excel de hojas de vida (la ruta es distinta en cada maquina)
    4. La conexion con PostgreSQL

No escribe nada en la base ni en las planillas: solo mira.
"""

import sys
from pathlib import Path

# Deja la raiz del proyecto en el path para poder importar `core` y `etl`
# aunque Spyder este parado en otra carpeta.
RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))

# El error mas frecuente al empezar en una maquina nueva es que Spyder este
# apuntando al interprete equivocado. Se avisa aca, antes de importar nada,
# para que salga un mensaje en vez de un traceback.
if sys.version_info < (3, 11):  # noqa: UP036  se comprueba a proposito
    print("Este proyecto necesita Python 3.11 o superior.")
    print(f"El interprete en uso es {sys.version.split()[0]}: {sys.executable}")
    print()
    print("En Spyder: Herramientas -> Preferencias -> Interprete de Python ->")
    print("'Usar el siguiente interprete de Python', y elegir:")
    print(r"    C:\Users\herma\anaconda3\python.exe")
    print("Despues, Consola -> Reiniciar kernel.")
    sys.exit(1)

from core import rutas  # noqa: E402
from core.bd import verificar_conexion  # noqa: E402
from core.config import ErrorDeConfiguracion, obtener_ajustes  # noqa: E402

BIEN = "[ OK ]"
MAL = "[FALTA]"


def _titulo(texto: str) -> None:
    print(f"\n{texto}")
    print("-" * len(texto))


def revisar_configuracion() -> bool:
    _titulo("1. Configuracion (.env)")
    try:
        ajustes = obtener_ajustes()
    except ErrorDeConfiguracion as error:
        print(f"{MAL} {error}")
        return False

    print(f"{BIEN} Archivo leido: {rutas.RAIZ_PROYECTO / '.env'}")
    print(f"       Libro de Google:  {ajustes.nombre_libro_google}")
    print(f"       Base de datos:    {_ocultar_clave(ajustes.url_base_datos)}")
    return True


def revisar_credenciales() -> bool:
    _titulo("2. Credenciales de Google")
    try:
        ruta = obtener_ajustes().exigir_credenciales_google()
    except ErrorDeConfiguracion as error:
        print(f"{MAL} {error}")
        return False
    print(f"{BIEN} {ruta}")
    return True


def revisar_excel() -> bool:
    _titulo("3. Excel de hojas de vida")
    try:
        ruta = obtener_ajustes().exigir_excel_hoja_de_vida()
    except ErrorDeConfiguracion as error:
        print(f"{MAL} {error}")
        return False

    tamano = ruta.stat().st_size / (1024 * 1024)
    print(f"{BIEN} {ruta}")
    print(f"       Tamano: {tamano:.1f} MB")
    print("       Recuerda: en el servidor esta ruta apunta al archivo original")
    print("       de la unidad de red, no a una copia.")
    return True


def revisar_base_de_datos() -> bool:
    _titulo("4. Base de datos")
    try:
        verificar_conexion()
    except ErrorDeConfiguracion as error:
        print(f"{MAL} {error}")
        return False
    print(f"{BIEN} PostgreSQL responde")
    return True


def revisar_librerias() -> bool:
    _titulo("5. Librerias")
    faltantes = []
    for libreria in ("gspread", "psycopg", "openpyxl", "sqlalchemy", "pydantic_settings"):
        try:
            __import__(libreria)
            print(f"{BIEN} {libreria}")
        except ImportError:
            print(f"{MAL} {libreria}")
            faltantes.append(libreria)

    if faltantes:
        print("\n       Instalalas con:")
        print(f"       {sys.executable} -m pip install {' '.join(faltantes)}")
    return not faltantes


def mostrar_donde_se_guarda_todo() -> None:
    _titulo("Donde queda cada cosa")
    for descripcion, directorio in (
        ("Lo leido, tal cual", rutas.CRUDOS),
        ("Ya normalizado", rutas.INTERMEDIOS),
        ("Lo que se carga", rutas.PROCESADOS),
        ("Registro de corridas", rutas.BITACORAS),
        ("Reportes", rutas.REPORTES),
    ):
        print(f"  {descripcion:<22} {directorio}")
    print("\n  Nada de eso se versiona: son datos del hospital, no codigo.")


def _ocultar_clave(url: str) -> str:
    """No imprime la contrasena de la base en pantalla."""
    if "@" not in url or "//" not in url:
        return url
    inicio, resto = url.split("//", 1)
    credenciales, servidor = resto.split("@", 1)
    usuario = credenciales.split(":", 1)[0]
    return f"{inicio}//{usuario}:***@{servidor}"


def main() -> int:
    print("=" * 60)
    print("HRC-CEMS  |  Verificacion del entorno")
    print("=" * 60)
    print(f"Proyecto:    {RAIZ_PROYECTO}")
    print(f"Interprete:  {sys.executable}")

    resultados = [
        revisar_configuracion(),
        revisar_credenciales(),
        revisar_excel(),
        revisar_base_de_datos(),
        revisar_librerias(),
    ]
    mostrar_donde_se_guarda_todo()

    _titulo("Resultado")
    if all(resultados):
        print("Todo listo: esta maquina puede correr el ETL.")
        return 0
    print("Hay cosas por resolver antes de correr el ETL. Ver los [FALTA] de arriba.")
    return 1


if __name__ == "__main__":
    codigo = main()
    # En Spyder conviene no matar la consola: solo se avisa el resultado.
    if codigo:
        print(f"\n(codigo de salida {codigo})")
