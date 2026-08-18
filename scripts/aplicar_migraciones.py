"""Aplica los cambios pendientes al esquema de la base de datos.

Para correrlo en Spyder: abrir este archivo y presionar F5.

Como funciona, en una frase: los archivos `.sql` de `database/migrations/` se
aplican en orden por su numero, y la base anota cuales ya aplico en la tabla
`migracion`. Volver a correr esto no repite nada.

Reglas de la fase 2, para no volver a discutirlas:

  - `database/schema/` es el estado inicial de la base. **No se edita nunca.**
  - Todo cambio posterior es un archivo nuevo y numerado en `migrations/`,
    que solo agrega. Nunca se corrige una migracion ya aplicada: se escribe
    la siguiente.
  - Sin Alembic. Saber en que estado esta una base es mirar una tabla.
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

from sqlalchemy import text  # noqa: E402

from core.bd import obtener_motor, verificar_conexion  # noqa: E402
from core.rutas import MIGRACIONES  # noqa: E402

TABLA_DE_CONTROL = """
CREATE TABLE IF NOT EXISTS migracion (
    id_migracion integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    archivo      text        NOT NULL UNIQUE,
    aplicada_en  timestamptz NOT NULL DEFAULT now()
)
"""


def migraciones_disponibles() -> list[Path]:
    """Los archivos .sql de la carpeta, en orden por su numero."""
    if not MIGRACIONES.is_dir():
        return []
    return sorted(MIGRACIONES.glob("*.sql"))


def migraciones_aplicadas(conexion) -> set[str]:
    filas = conexion.execute(text("SELECT archivo FROM migracion")).scalars().all()
    return set(filas)


def aplicar(archivo: Path) -> None:
    """Aplica una migracion dentro de una transaccion propia.

    Si el archivo falla a la mitad, no queda aplicado a medias ni se anota
    como aplicado: se corrige el .sql y se vuelve a correr.
    """
    contenido = archivo.read_text(encoding="utf-8")
    with obtener_motor().begin() as conexion:
        conexion.exec_driver_sql(contenido)
        conexion.execute(
            text("INSERT INTO migracion (archivo) VALUES (:archivo)"),
            {"archivo": archivo.name},
        )


def main() -> int:
    print("=" * 60)
    print("HRC-CEMS  |  Migraciones de la base de datos")
    print("=" * 60)

    verificar_conexion()

    with obtener_motor().begin() as conexion:
        conexion.exec_driver_sql(TABLA_DE_CONTROL)

    with obtener_motor().connect() as conexion:
        aplicadas = migraciones_aplicadas(conexion)

    disponibles = migraciones_disponibles()
    if not disponibles:
        print("\nNo hay migraciones escritas todavia.")
        return 0

    print(f"\nMigraciones escritas:  {len(disponibles)}")
    print(f"Ya aplicadas:          {len(aplicadas)}")

    pendientes = [archivo for archivo in disponibles if archivo.name not in aplicadas]
    if not pendientes:
        print("\nLa base ya esta al dia. No hay nada que aplicar.")
        return 0

    print(f"\nPendientes: {len(pendientes)}")
    for archivo in pendientes:
        print(f"  aplicando {archivo.name} ...", end=" ")
        aplicar(archivo)
        print("listo")

    print("\nBase actualizada.")
    return 0


if __name__ == "__main__":
    main()
