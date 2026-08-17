# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Qué es este proyecto

HRC-CEMS es el sistema de gestión de equipos médicos (EEMM) de la unidad de Ingeniería Clínica del Hospital Regional de Copiapó. El objetivo final es una web Flask desplegada en la LAN del hospital que muestre inventario, mantenimientos y análisis de fallas, alimentada por un ETL que extrae datos de Google Sheets y Excel.

El dominio, los datos y el código están **en español**: nombres de funciones, columnas y commits. Mantener esa convención.

## Estado actual: andamiaje vacío + código en `legacy/`

Esto es lo primero que hay que entender antes de tocar nada:

- `app/`, `etl/`, `core/`, `database/`, `analytics/`, `tests/` son **directorios vacíos** (solo `.gitkeep`). La arquitectura nueva está definida pero no implementada.
- **Todo el código que funciona está en `legacy/`**, que es el proyecto anterior archivado íntegro (scripts + sus datos + su documentación).
- No hay app Flask, no hay base de datos, no hay tests.

Al implementar algo nuevo, la fuente de la lógica de negocio es `legacy/scripts/`. Portar desde ahí, no reescribir a ciegas.

`legacy/` es **autocontenido**: sus scripts calculan `project_root` como `../..` desde su propia ubicación, lo que ahora resuelve a `legacy/`, y `legacy/data/` se movió junto con ellos. Por eso siguen ejecutándose correctamente sin modificarlos. No "arreglar" esas rutas.

## Entorno y comandos

**Python no está en el PATH.** El intérprete es Anaconda:

```bash
C:\Users\herma\anaconda3\python.exe
```

El autor trabaja en **Spyder** (de ahí el `.spyproject/` y scripts con código ejecutable a nivel de módulo). `environment.yml` quedó archivado en `legacy/`; el `requirements.txt` de la raíz es ahora la única fuente de verdad de dependencias.

```bash
C:\Users\herma\anaconda3\python.exe -m pip install -r requirements.txt
```

Lint y formato (ruff está declarado en requirements; no hay configuración propia todavía):

```bash
C:\Users\herma\anaconda3\python.exe -m ruff check .
```

Tests (`tests/unit/` y `tests/integration/` existen pero están vacíos):

```bash
C:\Users\herma\anaconda3\python.exe -m pytest
```

Un solo test:

```bash
C:\Users\herma\anaconda3\python.exe -m pytest tests/unit/test_x.py::test_caso -v
```

### Ejecutar el pipeline legacy

Desde `legacy/`, en este orden — la segunda etapa depende de los CSV que produce la primera:

```bash
cd legacy/scripts/a_data_import && python google_sheet_integration.py && python processing_raw_google_data.py
```

`analytics.py` es la excepción: importa `from scripts.a_data_import.utils import ...`, así que necesita `legacy/` como raíz de paquetes:

```bash
cd legacy && python -m scripts.b_data_analytics.analytics
```

## Arquitectura del ETL (el núcleo del proyecto)

El flujo legacy, que es el que hay que portar a `etl/`:

```
Google Sheets "EEMM" (10 hojas)     Excel HOJAS_DE_VIDA.xlsm
        │                                    │
        ▼ google_sheet_integration.py        ▼ processing_raw_excel_hdv.py
   data/raw/google_sheets/*_raw.csv
        │
        ▼ processing_raw_google_data.py  ── etapa 1: un process_<hoja>() por fuente
   data/processed/*_processed.csv
        │
        ▼ filter_by_catastro()           ── etapa 2: SOBREESCRIBE los processed
   data/processed/ (solo series válidas)
        │
        ▼ analytics.py / clustering_*.py
   data/processed/analytics/ + clusters/
```

Dos decisiones de diseño que hay que respetar al portar:

**`CATASTRO` es el registro autoritativo de equipos.** `filter_by_catastro()` cruza todos los procesados contra el conjunto de `SERIE` válidas del catastro y **sobreescribe los archivos en sitio**, borrando los que quedan sin coincidencias. Es destructivo e idempotente solo si se re-ejecuta la etapa 1 antes. Al reimplementar, preferir un directorio de salida separado en vez de sobreescribir.

**`SERIE` es la clave de unión universal**, normalizada siempre con `.strip().str.upper()`. Un `SERIE` puede venir con varios números en una celda: `dividir_y_agregar()` los separa (por espacio, `:`, `/`, `//`) y el DataFrame se `explode`a a una fila por equipo. Todos los CSV se leen con `dtype=str` para evitar que pandas infiera tipos sobre datos sucios del hospital.

**`id_unico`** se compone como `FECHA_SERIE_DOCUMENTO_TIPO` (fecha en `%Y-%m-%d`). Es el candidato natural a clave primaria al diseñar el esquema SQL.

Las fechas pasan por `convertir_fecha_estandar()` (en `legacy/scripts/a_data_import/utils.py`), que prueba 12 formatos y cae a `dayfirst=True` — los datos vienen en formato chileno `DD/MM/YYYY`.

## Glosario del dominio

Sin esto, los nombres de columnas y archivos son indescifrables. Cada hoja de Google Sheets produce un `TIPO` de evento:

| Sigla | Significado | `TIPO` asignado |
|---|---|---|
| `EEMM` | Equipos médicos (nombre del Google Sheet) | — |
| `CATASTRO` | Inventario autoritativo de equipos | — |
| `PMP` | Plan de mantenimiento preventivo | — |
| `OT` | Orden de trabajo (hoja `OT26`) | `ORDEN DE TRABAJO` |
| `AE` | Acta de entrega | `ENTREGA` |
| `AP` | Acta de préstamo | `PRESTAMO` |
| `CS` | Salida a servicio técnico externo | `SALIDA A SERVICIO TECNICO` |
| `IT` | Informe técnico | `INFORME TÉCNICO` |
| `HDV` | Hoja de vida del equipo (fuente Excel) | — |
| `AMFE` | Análisis modal de fallos y efectos | — |
| `IM` | Índice de mantenimiento (`IM>12` = umbral de criticidad) | — |
| `NIC` | Código de inventario del equipo | — |

En `PMP`, `CATEGORÍA` se mapea: `EC` → equipo crítico, `ER` → equipo relevante.

Los clusterings (`clustering_ot.py`, `clustering_problemas_eemm.py`) agrupan texto libre en español de `OBS CLÍNICA` / problemas reportados con TF-IDF + KMeans, con stopwords específicas del dominio hospitalario definidas en el propio script. Los archivos de salida llevan el silhouette score en el nombre (`OT_CLUSTERS_FINAL_score0.787.csv`).

## Datos, secretos y git

`.gitignore` ignora el **contenido** de `data/`, `logs/` y `reports/` pero conserva la estructura de directorios vía `.gitkeep` (patrón `data/**` + `!data/**/` + `!data/**/.gitkeep`). Al crear subdirectorios nuevos ahí, añadir su `.gitkeep`.

Fuera del repo y necesarios para que el ETL corra — hay que recrearlos en cada máquina:

- `.env` con `GOOGLE_CREDENTIALS_PATH` y `EXCEL_HOJA_DE_VIDA_PATH` (rutas relativas a la raíz del proyecto)
- `secrets/credentials.json`: service account de Google con acceso al Sheet `EEMM`

`legacy/data/` también está ignorado: se versiona el código del borrador anterior, no sus datos.

## Trampas conocidas

- `legacy/scripts/1. data_import/` es una copia **antigua** de `a_data_import/` (dos archivos difieren, ~100 líneas menos). Usar siempre `a_data_import/`.
- `legacy/scripts/b_data_analytics/analytics.py` importa `seaborn`, que **no está en `requirements.txt`**.
- El bloque `__main__` de `processing_raw_google_data.py` llama a `filter_by_catastro()` **dos veces** (líneas 363 y 372). Es inofensivo por idempotencia, pero no replicarlo al portar.
- `legacy/scripts/c_database_setup/setup_db.py` y `.py` son placeholders de 113 bytes, no implementación.
- Los scripts de clustering ejecutan código al importarse (estilo Spyder), no solo bajo `__main__`.
- El repo vive en OneDrive, lo que ocasionalmente deja un `.git/index.lock` huérfano que bloquea git.

## Documentación de referencia en `legacy/`

- `ORIENTACION_PROYECTO.md`: diagnóstico del estado anterior y hoja de ruta por fases (BD → API → despliegue LAN).
- `ESTRUCTURA_PROYECTO.md`: arquitectura propuesta con plantillas de `app.py` y `config.py`.
- `GUIA_BASES_DATOS.md`: comparativa SQLite/PostgreSQL/MySQL con la recomendación de **PostgreSQL** y los pasos de migración CSV → BD.

Ojo: describen la estructura *anterior* (`scripts/a_data_import/`, etc.), no la actual. El árbol de directorios raíz es la referencia válida.
