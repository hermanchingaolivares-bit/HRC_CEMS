"""Configuracion de pytest.

El proyecto no se instala como paquete, asi que sin esto las pruebas no
encuentran los modulos `core` ni `etl`.
"""

import sys
from pathlib import Path

RAIZ_PROYECTO = Path(__file__).resolve().parent

if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))
