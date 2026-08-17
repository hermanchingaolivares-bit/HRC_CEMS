# legacy/ — Archivo del prototipo anterior

Este directorio conserva el borrador previo de HRC-CEMS, anterior a la reestructuración del proyecto.

## No ejecutar

El código de `legacy/scripts/` es **material de referencia**, no una implementación en uso:

- No se ejecuta ni se mantiene.
- Queda excluido del linter y de las pruebas (ver `extend-exclude` en `pyproject.toml`).
- Contiene duplicados, rutas acopladas a la estructura antigua y llamadas repetidas.
- `c_database_setup/` son marcadores de posición vacíos, nunca hubo base de datos.

Los módulos nuevos (`etl/`, `app/`, `database/`) se escriben **desde cero** en la raíz del proyecto. Este directorio sirve para consultar cómo se resolvieron los problemas del dominio, no para copiar y pegar.

## Por qué se conserva

Documenta conocimiento que no está en ningún otro sitio:

- **Las reglas de normalización** de datos hospitalarios sucios: series múltiples en una celda, doce formatos de fecha distintos, claves compuestas.
- **La semántica de las siglas** del dominio (`PMP`, `OT`, `AE`, `AP`, `CS`, `IT`, `HDV`, `AMFE`, `IM`, `NIC`). El glosario está en el `CLAUDE.md` de la raíz.
- **El papel de `CATASTRO`** como inventario autoritativo contra el que se validan todas las demás fuentes.
- **Las particularidades de las fuentes**: nombres de hojas, columnas reales y sus inconsistencias.

## Contenido

| Ruta | Qué es |
|---|---|
| `scripts/a_data_import/` | Extracción de Google Sheets y Excel, y normalización. Versión más reciente |
| `scripts/1. data_import/` | Copia **antigua** de la anterior. Dos archivos difieren, unas 100 líneas menos |
| `scripts/b_data_analytics/` | Análisis descriptivo y clustering de texto libre con TF-IDF + KMeans |
| `scripts/c_database_setup/` | Marcadores vacíos: la base de datos nunca se implementó |
| `scripts/d_borradores/` | Borradores sueltos de Spyder |
| `data/` | Datos crudos y procesados del prototipo. **No versionado** |
| `environment.yml` | Entorno conda del prototipo. Sustituido por `requirements.txt` en la raíz |
| `ORIENTACION_PROYECTO.md` | Diagnóstico del estado anterior y hoja de ruta por fases |
| `ESTRUCTURA_PROYECTO.md` | Arquitectura propuesta con plantillas de `app.py` y `config.py` |
| `GUIA_BASES_DATOS.md` | Comparativa SQLite / PostgreSQL / MySQL y pasos de migración |

Los tres documentos `.md` describen la estructura *antigua* del repositorio. Para la estructura vigente, el `README.md` de la raíz es la referencia.
