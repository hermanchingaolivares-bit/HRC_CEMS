# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Qué es este proyecto

HRC-CEMS es el sistema de gestión de equipos médicos (EEMM) de la unidad de Ingeniería Clínica del Hospital Regional de Copiapó. El objetivo es una web Flask desplegada en la LAN del hospital que muestre inventario, mantenimientos y análisis de fallas, alimentada por un ETL que consolida datos de Google Sheets y Excel.

El dominio, el código y los commits están **en español**: nombres de funciones, columnas, mensajes. Mantener esa convención.

## Estado actual

**Fase 1 cerrada** (17-ago-2026) y **fase 2 en curso: el ETL**. Lo que existe:

- **Estructura y configuración**: árbol de directorios, `requirements.txt`, `pyproject.toml`, `.env.example`, `.gitattributes`.
- **Esquema de la base de datos**: `database/schema/` — `01_tablas.sql`, `02_disparadores.sql`, `03_vistas.sql`. Ya aplicado en la base `hrc_cems` de PostgreSQL local: 11 tablas y 2 vistas.
- **Documentación**: este archivo, el `README.md` y los documentos de orientación en `legacy/`.

Lo que **no** existe todavía: ni ETL, ni app Flask, ni pruebas, ni migraciones versionadas, ni semillas. Los directorios `app/`, `etl/`, `core/`, `analytics/` y `tests/` están vacíos (solo `.gitkeep`), y también `database/migrations/` y `database/seeds/`.

El ETL necesita `tipo_equipo` y `servicio_clinico` pobladas antes de poder cargar equipos: las semillas son el primer paso de la fase 2.

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

### Base de datos local

PostgreSQL 18.6 en el PC de desarrollo, servicio `postgresql-x64-18`, puerto 5432, locale `Spanish_Chile.utf8`. Binarios en `C:\Program Files\PostgreSQL\18\bin\`. La cadena de conexión está en `.env` (`DATABASE_URL`), fuera de git.

**El sandbox de la herramienta Bash bloquea la conexión a localhost.** Para `psql` usar la herramienta PowerShell; con Bash falla con "Connection refused" aunque el servidor esté arriba.

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

El motor es **PostgreSQL**, decidido en la fase 1 y no negociable a estas alturas: el esquema usa `UNIQUE ... NULLS NOT DISTINCT`, columnas generadas y disparadores en plpgsql, que SQLite no tiene. Nada de "SQLite para desarrollo local": desarrollo y hospital corren el mismo motor.

## Conocimiento del dominio

Las reglas que hacen funcionar el ETL. Algunas se rescatan del prototipo; otras lo **corrigen**, y esas están marcadas.

### Flujo previsto

```
Google Sheets "EEMM" (API)  +  Excel HOJAS_DE_VIDA.xlsm (original en el servidor)
                    │
                    ▼  extracción
              data/raw/
                    │
                    ▼  normalización, una transformación por fuente
              data/interim/
                    │
                    ▼  el PMP define el universo; el catastro aporta atributos
              data/processed/  →  base de datos
                    │
                    ▼
              analítica y reportes
```

### De dónde se leen las fuentes

**El Google Sheet se lee siempre por la API**, con `gspread` y la service account de `secrets/credentials.json`, tomando los valores **como texto** (`get_all_values()`, sin inferencia de tipos). Nunca desde una exportación a `.xlsx`: la exportación corrompe las series numéricas largas a notación científica. Los archivos de `legacy/Planillas (Referencilaes)/` son una foto para consulta, no una fuente del ETL. Como guía de la mecánica de conexión sirve `legacy/scripts/a_data_import/google_sheet_integration.py` — la mecánica, no su lista de hojas, que tiene errores (ver el glosario).

**El Excel de hojas de vida se lee directo del `.xlsm`**, no de una copia: en el PC que hará de servidor la ruta apunta al archivo original en la unidad de red, que la unidad abre y edita a diario. Esa ruta es `EXCEL_HOJA_DE_VIDA_PATH` en `.env` y **cambia según la máquina** — nunca fijarla en el código. En el PC de desarrollo puede apuntar a una copia local; en el servidor, al original.

**Los nombres de hoja cambian con el año** (`OT26` → `OT27`, `PMP 2024`…): no fijarlos como constantes, resolverlos por año o por patrón.

### Reglas que hay que respetar

**El universo de equipos lo define el `PMP`, no el `CATASTRO`.** Corrige al prototipo, que filtraba todo contra las series del catastro. Un equipo entra a la base de datos **cuando tiene plan de mantenimiento preventivo vigente** — crítico, relevante o IM>12 —, no por estar clasificado en el catastro. El catastro aporta los atributos (nombre, marca, modelo, ubicación, clasificación); la pertenencia la decide el plan. De ahí que `plan_mantenimiento` lleve el comentario "define el universo" y que `rechazo` tenga el motivo `SIN_PLAN`.

**El universo es el del año en curso y es móvil.** 781 equipos en 2026 (485 del `PMP` de EC/ER + 296 del `PMP IM>12`; los dos planes no comparten ningún equipo), contra 842 acumulados 2025-2027. Por clasificación del catastro habrían sido 1.645: la mitad de los clasificados no tiene plan. El plan se alimenta mes a mes, así que **el universo se recalcula en cada carga** y nunca se fija en una lista.

**Lo que no cuadra se reporta, no se borra.** El ETL parte de las dos hojas PMP, resuelve la serie contra el catastro y deja constancia de las discrepancias en `rechazo` (al 17-ago-2026: 31 series con plan que no existen en el catastro, 24 con plan que figuran como `NO APLICA`). El prototipo, en cambio, filtraba **sobreescribiendo los archivos procesados en sitio** y borrando los que quedaban vacíos: no reproducir eso — escribir a un directorio de salida distinto (de ahí `data/interim/`).

**`SERIE` es la clave de unión universal**, normalizada siempre con `strip` + `upper`. Una sola celda puede contener varias series separadas por espacio, `:`, `/` o `//`: hay que separarlas y expandir a una fila por equipo. Todo se lee como texto (`dtype=str`) para que pandas no infiera tipos sobre datos sucios.

**El `id_unico` del prototipo ya no se usa.** Se componía como `FECHA_SERIE_DOCUMENTO_TIPO`; su papel lo cumple ahora la restricción `hoja_de_vida_sin_duplicados` (`equipo_id, fecha, documento, tipo` con `NULLS NOT DISTINCT`), que hace idempotente volver a cargar la misma fuente.

**Las fechas vienen en formato chileno** (`DD/MM/YYYY`) pero con mucha variación: el prototipo probaba doce formatos antes de caer a inferencia con día primero. La lógica está en `legacy/scripts/a_data_import/utils.py` y merece reimplementarse con pruebas.

**El origen de los cambios se marca antes de escribir.** Los disparadores registran cada campo modificado en `cambio`; el ETL declara su procedencia con `SET LOCAL hrc.origen = 'ETL'` y `SET LOCAL hrc.carga_id = '<id>'` en la misma transacción. Sin eso, el cambio queda registrado como `MANUAL`. Cada corrida abre además una fila en `carga`.

### Glosario

**El glosario vigente está en Notion, no aquí**: "📖 Glosario del dominio" — https://app.notion.com/p/3bf0d2a2d70281d48d82df62c3bea663. Es la versión corregida por Herman en dos rondas el 17-ago-2026, y cubre las siglas de la unidad, las columnas de las planillas y qué hoja entra al sistema y cuál no. **Ante cualquier discrepancia con este archivo, manda Notion.** Consultarlo antes de modelar o de escribir ETL.

Abajo, lo mínimo para leer el código sin salir del repositorio. Cada hoja de origen produce un tipo de evento en `hoja_de_vida`:

| Sigla | Significado | Tipo de evento |
|---|---|---|
| `EEMM` | Equipos médicos (nombre del Google Sheet) | — |
| `CATASTRO` | Inventario de equipos: aporta atributos, no define el universo | — |
| `PMP` | Plan de mantenimiento preventivo | — |
| `OT` | Orden de trabajo (hoja `OT26` en 2026) | `ORDEN DE TRABAJO` |
| `AE` | Acta de entrega | `ENTREGA` |
| `AP` | Acta de préstamo | `PRESTAMO` |
| `CS` | Salida a servicio técnico externo | `SALIDA A SERVICIO TECNICO` |
| `IT` | Informe técnico | `INFORME TÉCNICO` |
| `HDV` | Hoja de vida del equipo (fuente Excel) | — |
| `AMFE` | Análisis modal de fallos y efectos (método, no hoja) | — |
| `IM` | Índice de mantenimiento (`IM>12` = umbral de criticidad) | — |
| `NIC` | Código de inventario del equipo | — |

En `PMP`, la columna `CATEGORÍA` codifica `EC` como equipo crítico y `ER` como equipo relevante.

Dos advertencias sobre el prototipo, que se equivocaba en ambas:

- **La hoja `AMFE EQUIPOS` no existe** en el Sheet. `legacy/.../google_sheet_integration.py` la lee igual y se traga el error en silencio. Las fallas vienen de `GESTION DE FALLAS`.
- **`LPF` y `NICOLE` son personas**, no siglas de documentos ni de hojas.

El Sheet tiene medio centenar de hojas; solo unas pocas entran al sistema. La lista vigente está en Notion, no aquí — este archivo no la duplica para no volver a quedar desfasado.

El clustering del prototipo agrupaba texto libre en español (observaciones clínicas, problemas reportados) con TF-IDF + KMeans, usando stopwords propias del dominio hospitalario. Los archivos de salida llevaban el silhouette score en el nombre.

## Datos, secretos y convenciones del repositorio

`.gitignore` ignora el **contenido** de `data/`, `logs/` y `reports/` pero conserva la estructura de directorios vía `.gitkeep` (patrón `data/**` + `!data/**/` + `!data/**/.gitkeep`). Al crear subdirectorios ahí, añadir su `.gitkeep`. `legacy/data/` también está ignorado.

Fuera del repositorio y necesarios para que el ETL funcione — hay que recrearlos en cada máquina:

- `.env`, a partir de `.env.example`. Ojo con `EXCEL_HOJA_DE_VIDA_PATH`: **es distinta en cada máquina** (copia local en desarrollo, archivo original de la unidad de red en el servidor).
- `secrets/credentials.json`: service account de Google con acceso al Sheet `EEMM`

`legacy/Planillas (Referencilaes)/` guarda una foto de las planillas para consultar su estructura sin conectarse. Son binarios de varios MB: **no se versionan** (ignorados en `.gitignore`) y no son fuente del ETL.

`.gitattributes` fija **LF** como final de línea en el repositorio. Se añadió porque el proyecto se trabaja desde varias máquinas y por la web, y sin él cada `core.autocrlf` local generaba avisos y diffs fantasma.

El repositorio vive en OneDrive, lo que ocasionalmente deja un `.git/index.lock` huérfano que bloquea git. Si aparece sin ningún proceso git vivo, se puede borrar.

## Documentación de referencia en `legacy/`

- `README.md`: qué contiene el archivo y por qué se conserva.
- `ORIENTACION_PROYECTO.md`: diagnóstico del prototipo y hoja de ruta por fases (BD → API → despliegue LAN).
- `ESTRUCTURA_PROYECTO.md`: arquitectura propuesta con plantillas de `app.py` y `config.py`.
- `GUIA_BASES_DATOS.md`: comparativa SQLite/PostgreSQL/MySQL y pasos de migración.

Los tres últimos describen la estructura *antigua* (`scripts/a_data_import/`, etc.). Para la estructura vigente, el árbol del `README.md` de la raíz es la referencia.
