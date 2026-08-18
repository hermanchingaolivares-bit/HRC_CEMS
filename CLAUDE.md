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

- **Corte 0 del ETL** (la base sobre la que se apoyan los demás cortes):
  - `core/`: `rutas.py` (dónde se guarda cada cosa), `config.py` (lee el `.env`), `bd.py`, `bitacora.py`.
  - `etl/transform/normalizar.py`: series, fechas chilenas y números, con 74 pruebas.
  - `etl/extract/`: `google_sheets.py` (API, todo texto) y `excel_hdv.py` (el `.xlsm` de 963 hojas).
  - `etl/load/registro.py`: el contexto `carga_en_curso`, que abre la fila en `carga`, declara la autoría del ETL y guarda los rechazos.
  - `scripts/verificar_entorno.py` y `scripts/leer_hoja_google.py`, pensados para correr con F5 desde Spyder.

Lo que **no** existe todavía: ni las transformaciones por fuente, ni la carga de cada tabla, ni app Flask, ni migraciones versionadas. Los directorios `app/`, `analytics/` y `etl/contracts/` están vacíos, y también `database/migrations/`.

Para ejecutar el proyecto desde Spyder y saber dónde queda cada dato: `docs/COMO_EJECUTAR.md`. El plan completo de la fase 2 esta en `docs/PLAN_FASE_2.md`: define una sola forma de hacer cada cosa y es lo que manda al escribir el ETL. Los catalogos `tipo_equipo` y `servicio_clinico` se cargan desde las planillas como cualquier otra fuente; no hay semillas escritas a mano y `database/seeds/` queda sin uso.

### `legacy/` es referencia, no código a ejecutar

Los scripts de `legacy/scripts/` son el prototipo anterior, conservado como **material de consulta del dominio**. No se ejecutan, no se mantienen y no se portan línea a línea. Están excluidos del linter (`extend-exclude` en `pyproject.toml`).

Los módulos nuevos se escriben **desde cero**. Consultar `legacy/` para entender las reglas del negocio y las peculiaridades de las fuentes de datos; no para copiar implementaciones. Ver `legacy/README.md`.

Corolario práctico: **no invertir esfuerzo en arreglar nada dentro de `legacy/`** — ni rutas, ni imports, ni dependencias que le falten. Que ese código no corra no es un defecto que haya que reparar.

## Entorno y comandos

**Todo vive en un solo entorno de conda: `cems`**, con Python 3.13. Ahí están las librerías del ETL, las pruebas, el linter y el propio Spyder. Nada del proyecto se instala fuera de ese entorno, y Python no está en el PATH, así que el intérprete se invoca por ruta completa:

```bash
C:\Users\herma\anaconda3\envs\cems\python.exe
```

El proyecto declara `requires-python = ">=3.11"`; `core/__init__.py` corta la ejecución con un mensaje claro si se usa un intérprete anterior, que era el error más frecuente al abrir Spyder con el entorno equivocado.

`requirements.txt` es la **única** lista de dependencias: si falta una librería se agrega ahí, nunca se instala suelta. `environment.yml` quedó archivado en `legacy/` y no se usa. Recrear el entorno en otra máquina son dos comandos, documentados en `docs/COMO_EJECUTAR.md`:

```bash
conda create -n cems python=3.13 -y && conda activate cems && pip install -r requirements.txt spyder
```

`requirements-ml.txt` contiene el NLP pesado (torch, transformers) y solo se instala si se retoma el clustering semántico.

Lint, formato y pruebas — configurados todos en `pyproject.toml`:

```bash
C:\Users\herma\anaconda3\envs\cems\python.exe -m ruff check .
```

```bash
C:\Users\herma\anaconda3\envs\cems\python.exe -m pytest
```

Una prueba concreta:

```bash
C:\Users\herma\anaconda3\envs\cems\python.exe -m pytest tests/unit/test_x.py::test_caso -v
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
| `core/` | Configuración, entorno, rutas de salida y registro de actividad |
| `database/` | Esquema SQL y migraciones |
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

**Los dos planes son excluyentes.** Un equipo está en el de acreditación (crítico o relevante) o en el de `IM>12`, nunca en ambos. La exclusividad no se controla con una restricción propia: sale de que `tipo_equipo.categoria` es obligatoria y única por tipo.

**Salir del plan no es salir de inmediato.** Un equipo se sigue mostrando **hasta dos años** después de dejar el plan; si en dos años no vuelve a ninguno, pasa a registros históricos. Eso es exactamente lo que resuelve la vista `equipo_vigente`. Y **la baja saca al equipo de la vista activa, pero nunca se borra**.

**Las planillas no se tocan.** No se cambia la forma de trabajar de la unidad: el ETL se adapta a los datos como vienen, no al revés. Corolario: los defectos de dato se reportan en `rechazo` para que la unidad los corrija en su planilla, y el ETL no los "arregla" por su cuenta.

**La fuente va a moverse de Google Sheets a OneDrive**, sin fecha definida. La capa de extracción tiene que poder cambiar sin arrastrar a las de transformación y carga.

**Lo que no cuadra se reporta, no se borra.** El ETL parte de las dos hojas PMP, resuelve la serie contra el catastro y deja constancia de las discrepancias en `rechazo` (al 17-ago-2026: 31 series con plan que no existen en el catastro, 24 con plan que figuran como `NO APLICA`). El prototipo, en cambio, filtraba **sobreescribiendo los archivos procesados en sitio** y borrando los que quedaban vacíos: no reproducir eso — escribir a un directorio de salida distinto (de ahí `data/interim/`).

**`SERIE` es la clave de unión universal**, normalizada siempre con `strip` + `upper`. Una sola celda puede contener varias series separadas por espacio, `:`, `/` o `//`: hay que separarlas y expandir a una fila por equipo. Todo se lee como texto (`dtype=str`) para que pandas no infiera tipos sobre datos sucios.

**La serie es única, y sus defectos son defectos de dato.** Un equipo sin serie no entra; una serie repetida se rechaza y se reporta (`SERIE_VACIA`, `SERIE_DUPLICADA`). No son casos a modelar. Y **el `NIC` no sirve de identificador de respaldo**, porque la mayoría de los equipos no tiene uno.

**No toda orden de trabajo tiene equipo asociado**, y ese trabajo no se pierde: `orden_trabajo.equipo_id` admite nulo y `equipo_texto` guarda lo que escribió la unidad, para poder asociarlo después.

**El `id_unico` del prototipo ya no se usa.** Se componía como `FECHA_SERIE_DOCUMENTO_TIPO`; su papel lo cumple ahora la restricción `hoja_de_vida_sin_duplicados` (`equipo_id, fecha, documento, tipo` con `NULLS NOT DISTINCT`), que hace idempotente volver a cargar la misma fuente.

**Las fechas vienen en formato chileno** (`DD/MM/YYYY`) pero con mucha variación: el prototipo probaba doce formatos antes de caer a inferencia con día primero. La lógica está en `legacy/scripts/a_data_import/utils.py` y merece reimplementarse con pruebas.

**El origen de los cambios se marca antes de escribir.** Los disparadores registran cada campo modificado en `cambio`; el ETL declara su procedencia con `SET LOCAL hrc.origen = 'ETL'` y `SET LOCAL hrc.carga_id = '<id>'` en la misma transacción. Sin eso, el cambio queda registrado como `MANUAL`. Cada corrida abre además una fila en `carga`.

### Glosario

**La fuente del glosario es Notion, no este archivo**: "📖 Glosario del dominio" — https://app.notion.com/p/3bf0d2a2d70281d48d82df62c3bea663, y su continuación "🧱 Modelo de datos" — https://app.notion.com/p/3bf0d2a2d702818b8469eeafd0a71df0. Cerrado y validado por Herman el 17-ago-2026. **Ante cualquier discrepancia con este archivo, manda Notion.** Consultarlo antes de modelar o de escribir ETL; el modelo de datos de esa página corresponde uno a uno con `database/schema/`.

Abajo, lo mínimo para leer el código sin salir del repositorio.

**Identificadores.** `SERIE` es el número de serie del fabricante, única por equipo; si un equipo no tiene, se le asigna un código con estructura de NIC. `NIC` es el código de inventario interno de EEMM (`CDT-R02-07`, `NEO-029-12`, `IM001-23`) y no todos lo tienen. `N° INVENTARIO` es el código de la unidad de inventario del hospital, que es otra unidad. `SUB EQUIPO` es un componente con serie propia que pertenece a otro equipo (`equipo_padre_id`).

**Ubicación.** `SERVICIO CLÍNICO`, `UNIDAD` y `SERVICIO` son el mismo concepto con tres rótulos según la planilla. `RECINTO (SECTOR)` es el edificio; `RECINTO`, la sala.

**Documentos.** Correlativo `SIGLA-número-año` (`AE-001-15`). Las OT son la excepción: `OT260108` = `OT` + año + correlativo.

| Sigla | Significado | Tipo en `hoja_de_vida` |
|---|---|---|
| `OT` | Orden de Trabajo: la falla que reporta la unidad clínica y el trabajo que hizo EEMM | tabla propia |
| `AE` | Acta de Entrega (incluye traspasos) | `ENTREGA` |
| `AP` | Acta de Préstamo: cesión temporal entre unidades | `PRESTAMO` |
| `AC` | Acta de Capacitación del proveedor a la unidad | `CAPACITACION` |
| `CS` | Certificado de Salida: trazabilidad de equipos que salen del hospital | `SALIDA_SERVICIO_TECNICO` |
| `IT` | Informe Técnico | `INFORME_TECNICO` |
| `GD` | Guía de Despacho: recepción desde el proveedor | `RECEPCION` |
| `GS` | Guía de Servicio: mantenimiento hecho por la unidad; su código se anota en el `PMP` | — |
| `SEM` | Solicitud de Equipos Médicos a la subdirección de operaciones | — |
| `CC` | Certificado de Conformidad, para que contabilidad pague al proveedor | — |
| `NS-TV` | Notas de Seguridad y Tecnovigilancia (fabricante o ISP) | — |
| `LV` / `LV2` | Licitaciones vigentes: ítems adjudicados y consumo del monto | — |

**Mantenimiento.** `PMP` son **dos planes excluyentes**: el de acreditación (críticos y relevantes) y el de `IM>12`. `FP` es la fecha programada, casi siempre solo el mes; la ventana de tolerancia es `FP-1`/`FP+1`, y es lo que calcula la vista `plan_cumplimiento`. `FRECUENCIA` es la cantidad de mantenimientos al año (`1` anual, `2` semestral, `3`, `4`, o `1/2` para año por medio). `MP`/`MC` son mantenimiento preventivo y correctivo. El `ESTADO` recorre `1. ABIERTO` → `2. REALIZADO`/`REPROGRAMADO` → `3. INFORME A HDV` → `4. ENVIADO A CALIDAD` → `5. RECIBIDO DE CALIDAD` → `6. CERRADO`; la `SITUACIÓN` es el resultado de la ejecución (`EJECUTADO`, `EJECUTADO C/O`, o `NE` con su causa).

**Criticidad.** El ministerio define **qué tipos de equipo** son críticos y relevantes con un listado de nombres; **no define índices**. `EC`/`ER` son equipo crítico y relevante (`ECER` los contrae) y son las categorías que se acreditan. El `IM` es el índice de Fennigkoh y Smith — `FUNCIÓN`, `MANTENIMIENTO`, `RIESGO FÍSICO`, `ANTECEDENTES` — y se aplica a todos los equipos; a críticos y relevantes se les asignó 22 y 19 para reunirlos en un mismo índice. `IM>12` son los equipos con índice entre 12 y 18: quedan fuera de la acreditación porque su tipo no está en el listado ministerial, pero requieren plan igual, y por eso tienen un `PMP` propio.

**Gestión de fallas.** La planilla `AMFE` **pasó a llamarse `GESTIÓN DE FALLAS`** cuando el enfoque cambió de preventivo a reactivo. El `RPN` es Severidad × Ocurrencia × **Impacto**, donde el Impacto reemplaza a propósito a la Detección del AMFE clásico.

**Hojas de vida.** `HDV` es el historial de intervenciones, y vive en dos lugares que **no son copias**: el Excel `HOJAS_DE_VIDA.xlsm` tiene el historial profundo, con una hoja por equipo nombrada con el NIC o la serie; las hojas `HDV ECER` y `HDV IM≥12` del Sheet tienen solo el último año de algunos equipos. Cada intervención puede tener un `respaldo` documental: en críticos y relevantes está mayoritariamente físico y archivado; en los IM puede existir o no.

Nota que no está en el glosario pero corrigió Herman: **`LPF` y `NICOLE` son personas**, no siglas de documentos ni de hojas.

### Qué hojas entran al sistema

El Sheet tiene medio centenar de hojas y solo una parte alimenta la base. La lista vigente está en Notion; esta es la de la fase 1:

| Fuente | Qué aporta |
|---|---|
| `PMP` · `PMP IM>12` | Los dos planes; definen el universo |
| `CATASTRO` | Atributos de los equipos |
| `INDICES Y COSTOS` | Índice de mantenimiento por tipo de equipo → catalogo `tipo_equipo` |
| `Datos_Unidades` · `Agenda` | Lista de unidades del hospital y contacto de cada una → catalogo `servicio_clinico` |
| `OT26` | Órdenes de trabajo |
| `HDV ECER` · `HDV IM≥12` · `HOJAS_DE_VIDA.xlsm` | Hojas de vida |
| `GESTION DE FALLAS` | Fallas con RPN, criticidad y costo |
| `EQ. DE BAJA` | Equipos dados de baja |
| `AE` · `AP` · `CS` · `IT` · `AC` · `GD` | Documentos asociados a un equipo |

`CLAVES` no alimenta ninguna tabla todavía: son claves de acceso y códigos de servicio **de los equipos** (`EQUIPO`, `PROBLEMA`, `CODIGO`), no contactos de unidades.

Fuera de alcance por ahora: la gestión administrativa (`SEM`, `CC`, `Convenios`, `LV`, `LV2`, `NS-TV`, `PROYECTOS`), el inventario de dispositivos y accesorios (`DETALLE`, `ENTREGAS`, `RESUMEN`), y las hojas históricas o en desuso (`OT`, `PMP 2021`–`2024`, `OT PMP 2020`, `GS 2017`, `GS`, `ME`, `MP`, `SC`, `TE`, `PI`, `RE`, `Parámetros`, `Datos`, `Revisión_Protocolos`, `Anexos ACRE`, `HISTORIA`, `DashBoard`, `Funcionarios EEMM`).

**La hoja `AMFE EQUIPOS` no existe.** `legacy/scripts/a_data_import/google_sheet_integration.py` la lee igual y se traga el error en silencio: es el nombre viejo de `GESTION DE FALLAS`. Su lista de hojas no es de fiar.

El clustering del prototipo agrupaba texto libre en español (observaciones clínicas, problemas reportados) con TF-IDF + KMeans, usando stopwords propias del dominio hospitalario. Los archivos de salida llevaban el silhouette score en el nombre.

## Datos, secretos y convenciones del repositorio

`.gitignore` ignora el **contenido** de `data/`, `logs/` y `reports/` pero conserva la estructura de directorios vía `.gitkeep` (patrón `data/**` + `!data/**/` + `!data/**/.gitkeep`). **Ningún módulo arma rutas de salida por su cuenta**: todas salen de `core/rutas.py`, que además crea el `.gitkeep` al vuelo cuando aparece un subdirectorio nuevo. `legacy/data/` también está ignorado.

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
