"""Configuracion, entorno, rutas de salida y registro de actividad."""

import sys

INTERPRETE_DEL_PROYECTO = r"C:\Users\herma\anaconda3\envs\hrc-cems\python.exe"

# El proyecto necesita Python 3.11 o superior. La causa tipica de este error es
# tener Spyder apuntando al entorno viejo del prototipo, que trae Python 3.9.
if sys.version_info < (3, 11):  # noqa: UP036  se comprueba a proposito
    raise RuntimeError(
        f"Este proyecto necesita Python 3.11 o superior, y se esta usando "
        f"{sys.version.split()[0]}.\n"
        f"Interprete en uso: {sys.executable}\n\n"
        "En Spyder: Herramientas -> Preferencias -> Interprete de Python ->\n"
        "'Usar el siguiente interprete de Python', y elegir:\n"
        f"    {INTERPRETE_DEL_PROYECTO}\n"
        "Despues, Consola -> Reiniciar kernel."
    )
