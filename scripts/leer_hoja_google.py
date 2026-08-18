"""Lee una hoja del Google Sheet y deja una copia en `data/raw/`.

Sirve para mirar como llega una hoja antes de escribir su transformacion.
No carga nada a la base de datos.

Para correrlo en Spyder: cambiar `HOJA` por la hoja que se quiera mirar y
presionar F5.
"""

import sys
from pathlib import Path

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

from core.bitacora import configurar_bitacora  # noqa: E402
from etl.extract import google_sheets  # noqa: E402

# Que hoja leer. Las que alimentan la base, segun el plan de la fase 2:
#   "INDICES Y COSTOS"  "Datos_Unidades"  "Agenda"
#   "PMP"  "PMP IM>12"  "CATASTRO"
#   "OT26"  "GESTION DE FALLAS"  "EQ. DE BAJA"
HOJA = "INDICES Y COSTOS"

# Fila donde esta el encabezado, contada como la muestra la planilla.
FILA_ENCABEZADO = 1

# Cuantas filas mostrar en pantalla.
FILAS_A_MOSTRAR = 5


def main() -> None:
    registro = configurar_bitacora("leer_hoja_google")

    libro = google_sheets.abrir_libro()
    filas = google_sheets.leer_hoja(libro, HOJA, fila_encabezado=FILA_ENCABEZADO)

    if not filas:
        registro.warning("La hoja '%s' no devolvio filas", HOJA)
        return

    print(f"\nHoja '{HOJA}': {len(filas)} filas, {len(filas[0])} columnas")
    print("\nColumnas:")
    for nombre in filas[0]:
        print(f"  - {nombre}")

    print(f"\nPrimeras {FILAS_A_MOSTRAR} filas:")
    for fila in filas[:FILAS_A_MOSTRAR]:
        print(" ", {clave: valor for clave, valor in fila.items() if valor})

    archivo = google_sheets.guardar_copia_cruda(filas, HOJA)
    print(f"\nCopia guardada en: {archivo}")
    print("Esa copia es evidencia de la corrida, no una fuente: el ETL nunca lee de ahi.")


if __name__ == "__main__":
    main()
