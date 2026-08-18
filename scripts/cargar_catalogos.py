"""Carga los catalogos a la base: `tipo_equipo` y `servicio_clinico`.

Para correrlo en Spyder: abrir este archivo y presionar F5.

Es el primer script que **escribe** en la base de datos. Antes de correrlo
tiene que estar aplicada la migracion 001, que agrega los dos motivos de
rechazo que usan los catalogos:

    F5 sobre scripts/aplicar_migraciones.py

Que hace, en orden:

    1. Lee las tres hojas del Sheet y deja su copia cruda en data/raw/
    2. Las transforma segun las reglas del dominio
    3. Guarda en data/processed/ lo que va a cargar
    4. Escribe en la base, una transaccion por fuente
    5. Anota en `rechazo` lo que no se pudo cargar, con su motivo

Correrlo dos veces no duplica nada: si el tipo o la unidad ya existen, se
actualizan sus datos.
"""

import sys
from pathlib import Path

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))

if sys.version_info < (3, 11):  # noqa: UP036  se comprueba a proposito
    print("Este proyecto necesita Python 3.11 o superior.")
    print(f"El interprete en uso es {sys.version.split()[0]}: {sys.executable}")
    print()
    print("En Spyder: Herramientas -> Preferencias -> Interprete de Python ->")
    print("'Usar el siguiente interprete de Python', y elegir:")
    print(r"    C:\Users\herma\anaconda3\envs\cems\python.exe")
    print("Despues, Consola -> Reiniciar kernel.")
    sys.exit(1)

from core.bd import verificar_conexion  # noqa: E402
from core.bitacora import configurar_bitacora  # noqa: E402
from etl.extract import google_sheets  # noqa: E402
from etl.load import catalogos  # noqa: E402
from etl.load.archivos import guardar_procesado  # noqa: E402
from etl.load.registro import Carga, carga_en_curso  # noqa: E402
from etl.transform import servicio_clinico, tipo_equipo  # noqa: E402
from etl.transform.resultado import Resultado  # noqa: E402


def _volcar_rechazos(carga: Carga, resultado: Resultado) -> None:
    """Pasa a la tabla `rechazo` lo que la transformacion no pudo usar."""
    carga.contar_leidas(resultado.leidas)
    for rechazo in resultado.rechazos:
        carga.rechazar(rechazo.motivo, valor=rechazo.valor, detalle=rechazo.detalle)


def _mostrar(fuente: str, resultado: Resultado) -> None:
    print(f"\n{fuente}")
    print("-" * len(fuente))
    print(f"  {resultado.resumen()}")
    if resultado.rechazos:
        print("  Rechazos:")
        for rechazo in resultado.rechazos[:10]:
            print(f"    {rechazo.motivo:<18} {rechazo.valor!r}  {rechazo.detalle or ''}")
        if len(resultado.rechazos) > 10:
            print(f"    ... y {len(resultado.rechazos) - 10} mas, todos en la tabla rechazo")


def cargar_tipos_de_equipo(libro) -> Resultado:
    """INDICES Y COSTOS -> tipo_equipo."""
    filas = google_sheets.leer_hoja(libro, tipo_equipo.FUENTE)
    google_sheets.guardar_copia_cruda(filas, tipo_equipo.FUENTE)

    resultado = tipo_equipo.transformar(filas)
    guardar_procesado(resultado.filas, "tipo_equipo")

    with carga_en_curso(tipo_equipo.FUENTE) as carga:
        _volcar_rechazos(carga, resultado)
        carga.contar_cargadas(catalogos.cargar_tipos_de_equipo(carga, resultado.filas))

    return resultado


def cargar_servicios_clinicos(libro) -> Resultado:
    """Datos_Unidades + Agenda -> servicio_clinico."""
    unidades = google_sheets.leer_hoja(
        libro, servicio_clinico.HOJA_UNIDADES, servicio_clinico.FILA_ENCABEZADO_UNIDADES
    )
    agenda = google_sheets.leer_hoja(
        libro, servicio_clinico.HOJA_AGENDA, servicio_clinico.FILA_ENCABEZADO_AGENDA
    )
    google_sheets.guardar_copia_cruda(unidades, servicio_clinico.HOJA_UNIDADES)
    google_sheets.guardar_copia_cruda(agenda, servicio_clinico.HOJA_AGENDA)

    resultado = servicio_clinico.transformar(unidades, agenda)
    guardar_procesado(resultado.filas, "servicio_clinico")

    with carga_en_curso(servicio_clinico.FUENTE) as carga:
        _volcar_rechazos(carga, resultado)
        carga.contar_cargadas(catalogos.cargar_servicios_clinicos(carga, resultado.filas))

    return resultado


def main() -> None:
    configurar_bitacora("cargar_catalogos")

    print("=" * 60)
    print("HRC-CEMS  |  Carga de los catalogos")
    print("=" * 60)

    verificar_conexion()
    libro = google_sheets.abrir_libro()

    tipos = cargar_tipos_de_equipo(libro)
    servicios = cargar_servicios_clinicos(libro)

    _mostrar(tipo_equipo.FUENTE, tipos)
    _mostrar(servicio_clinico.FUENTE, servicios)

    print("\n" + "=" * 60)
    print("Listo. Para revisar en la base:")
    print("  SELECT categoria, count(*) FROM tipo_equipo GROUP BY categoria;")
    print("  SELECT count(*) FROM servicio_clinico;")
    print("  SELECT fuente, motivo, count(*) FROM rechazo GROUP BY fuente, motivo;")


if __name__ == "__main__":
    main()
