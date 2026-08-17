# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Qué es este proyecto

HRC-CEMS es el sistema de gestión de equipos médicos (EEMM) de la unidad de Ingeniería Clínica del Hospital Regional de Copiapó. El objetivo es una web Flask desplegada en la LAN del hospital que muestre inventario, mantenimientos y análisis de fallas, alimentada por un ETL que consolida datos de Google Sheets y Excel.

El dominio, el código y los commits están **en español**: nombres de funciones, columnas, mensajes. Mantener esa convención.

## Estado actual

El proyecto está en fase de acondicionamiento. Lo que existe:

- **Estructura y configuración**: árbol de directorios, `requirements.txt`, `pyproject.toml`, `.env.example`, `.gitattributes`.
- **Documentación**: este archivo, el `README.md` y los documentos de orientación en `legacy/`.

Lo que **no** existe todavía: ni ETL, ni base de datos, ni app Flask, ni pruebas. Los directorios `app/`, `etl/`, `core/`, `database/`, `analytics/` y `tests/` están vacíos (solo `.gitkeep`).

### `legacy/` es referencia, no código a ejecutar

Los scripts de `legacy/scripts/` son el prototipo anterior, conservado como **material de consulta del dominio**. No se ejecutan, no se mantienen y no se portan línea a línea. Están excluidos del linter (`extend-exclude` en `pyproject.toml`).

Los módulos nuevos se escriben **desde cero**. Consultar `legacy/` para entender las reglas del negocio y las peculiaridades de las fuentes de datos; no para copiar implementaciones. Ver `legacy/README.md`.

Corolario práctico: **no invertir esfuerzo en arreglar nada dentro de `legacy/`** — ni rutas, ni imports, ni dependencias que le falten. Que ese código no corra no es un defecto que haya que reparar.

## Entorno y comandos

**Python no está en el PATH.** El intérprete de desarrollo es Anaconda con Python 3.13:

```bash
C:\Users\herma\anaconda3\python.exe
```

El proyecto declara `requires-python = ">=3.11"`. `environment.yml` quedó archivado en `legacy/`; las dependencias se gestionan con pip.

```bash
C:\Users\herma\anaconda3\python.exe -m pip install -r requirements.txt
```

`requirements-ml.txt` contiene el NLP pesado (torch, transformers) y solo se instala si se retoma el clustering semántico.

Lint, formato y pruebas — configurados todos en `pyproject.toml`:

```bash
C:\Users\herma\anaconda3\python.exe -m ruff check .
```

```bash
C:\Users\herma\anaconda3\python.exe -m pytest
```

Una prueba concreta:

```bash
C:\Users\herma\anaconda3\python.exe -m pytest tests/unit/test_x.py::test_caso -v
```

Las pruebas que toquen Google Sheets o la base de datos van marcadas con `@pytest.mark.integration` (marcador declarado en `pyproject.toml`; `--strict-markers` está activo, así que un marcador sin declarar es un error).

## Arquitectura objetivo

Separación en capas, cada una con una responsabilidad:

| Directorio | Responsabilidad |
|---|---|
| `etl/extract/` | Lectura de fuentes: Google Sheets, Excel |
| `etl/transform/` | Normalización y validación |
| `etl/load/` | Carga a la base de datos |
| `etl/contracts/` | Contratos de datos (pydantic) |
| `core/` | Configuración, entorno, utilidades transversales |
| `database/` | Esquema SQL, migraciones, semillas |
| `app/` | Flask: `routes/` → `services/` → `models/`, con `templates/` y `static/` |
| `analytics/` | Modelos de análisis y notebooks |

Las rutas de Flask no deben contener lógica de negocio: reciben parámetros, llaman a un servicio y devuelven JSON o plantilla.

Sobre el motor de base de datos: `legacy/GUIA_BASES_DATOS.md` recomienda **PostgreSQL** para el despliegue en el hospital, con SQLite solo para desarrollo local. `requirements.txt` trae SQLAlchemy y deja los drivers (`psycopg`, `pyodbc`) comentados para descomentar según el motor elegido.

## Conocimiento del dominio a reimplementar

Esto es lo que hay que rescatar del prototipo. Son las reglas que hacen funcionar el ETL, no detalles de implementación.

### Flujo previsto

```
Google Sheets "EEMM" (10 hojas)  +  Excel HOJAS_DE_VIDA.xlsm
                    │
                    ▼  extracción
              data/raw/
                    │
                    ▼  normalización, una transformación por fuente
              data/interim/
                    │
                    ▼  validación contra CATASTRO
              data/processed/  →  base de datos
                    │
                    ▼
              analítica y reportes
```

### Reglas que hay que respetar

**`CATASTRO` es el registro autoritativo de equipos.** Toda fila de cualquier otra fuente se valida contra el conjunto de `SERIE` presentes en el catastro; lo que no coincide se descarta. El prototipo hacía este filtrado **sobreescribiendo los archivos procesados en sitio** y borrando los que quedaban vacíos: no reproducir eso, escribir a un directorio de salida distinto (de ahí que exista `data/interim/`).

**`SERIE` es la clave de unión universal**, normalizada siempre con `strip` + `upper`. Una sola celda puede contener varias series separadas por espacio, `:`, `/` o `//`: hay que separarlas y expandir a una fila por equipo. Los CSV del hospital se leen con `dtype=str` para que pandas no infiera tipos sobre datos sucios.

**`id_unico`** se componía como `FECHA_SERIE_DOCUMENTO_TIPO`, con la fecha en `%Y-%m-%d`. Es el candidato natural a clave primaria del esquema SQL.

**Las fechas vienen en formato chileno** (`DD/MM/YYYY`) pero con mucha variación: el prototipo probaba doce formatos antes de caer a inferencia con día primero. La lógica está en `legacy/scripts/a_data_import/utils.py` y merece reimplementarse con pruebas.

### Glosario

Sin esto los nombres de columnas y hojas son indescifrables. Cada hoja de origen produce un tipo de evento:

| Sigla | Significado | Tipo de evento |
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

En `PMP`, la columna `CATEGORÍA` codifica `EC` como equipo crítico y `ER` como equipo relevante.

El clustering del prototipo agrupaba texto libre en español (observaciones clínicas, problemas reportados) con TF-IDF + KMeans, usando stopwords propias del dominio hospitalario. Los archivos de salida llevaban el silhouette score en el nombre.

## Datos, secretos y convenciones del repositorio

`.gitignore` ignora el **contenido** de `data/`, `logs/` y `reports/` pero conserva la estructura de directorios vía `.gitkeep` (patrón `data/**` + `!data/**/` + `!data/**/.gitkeep`). Al crear subdirectorios ahí, añadir su `.gitkeep`. `legacy/data/` también está ignorado.

Fuera del repositorio y necesarios para que el ETL funcione — hay que recrearlos en cada máquina:

- `.env`, a partir de `.env.example`
- `secrets/credentials.json`: service account de Google con acceso al Sheet `EEMM`

`.gitattributes` fija **LF** como final de línea en el repositorio. Se añadió porque el proyecto se trabaja desde varias máquinas y por la web, y sin él cada `core.autocrlf` local generaba avisos y diffs fantasma.

El repositorio vive en OneDrive, lo que ocasionalmente deja un `.git/index.lock` huérfano que bloquea git. Si aparece sin ningún proceso git vivo, se puede borrar.

## Documentación de referencia en `legacy/`

- `README.md`: qué contiene el archivo y por qué se conserva.
- `ORIENTACION_PROYECTO.md`: diagnóstico del prototipo y hoja de ruta por fases (BD → API → despliegue LAN).
- `ESTRUCTURA_PROYECTO.md`: arquitectura propuesta con plantillas de `app.py` y `config.py`.
- `GUIA_BASES_DATOS.md`: comparativa SQLite/PostgreSQL/MySQL y pasos de migración.

Los tres últimos describen la estructura *antigua* (`scripts/a_data_import/`, etc.). Para la estructura vigente, el árbol del `README.md` de la raíz es la referencia.
