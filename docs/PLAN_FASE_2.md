# Plan de la fase 2: el ETL

Este documento define **una sola forma de hacer cada cosa**. No propone alternativas ni deja decisiones abiertas: si más adelante hay que cambiar algo, se cambia aquí y se cambia entero. Lo que no está escrito en este plan, no se hace.

Contexto: la fase 1 dejó el esquema aplicado en la base `hrc_cems`. La fase 2 llena esa base leyendo las planillas de la unidad.

## 1. Antes de escribir código

El proyecto corre en **un solo entorno de conda, `cems`**, con Python 3.13, donde está todo instalado: las librerías del ETL, las pruebas y el propio Spyder. Su intérprete es `C:\Users\herma\anaconda3\envs\cems\python.exe`.

`requirements.txt` es la única lista de dependencias. Si falta una librería se agrega ahí; nada se instala suelto. Crear el entorno en otra máquina son dos comandos, documentados en `docs/COMO_EJECUTAR.md`.

## 2. De dónde se lee cada fuente

**Google Sheets: siempre por la API.** Con la service account de `secrets/credentials.json`, tomando los valores como texto. Nunca desde un archivo exportado: la exportación convierte las series numéricas largas a notación científica y las corrompe.

**Excel de hojas de vida: siempre el archivo original.** La ruta se lee de `EXCEL_HOJA_DE_VIDA_PATH` en `.env` y **cambia según el computador**:

- PC de desarrollo: una copia local.
- PC servidor del hospital: el archivo original en la unidad de red, que la unidad abre y edita todos los días.

El código nunca sabe cuál de las dos es: pide la ruta a la configuración y lee lo que haya ahí. Cambiar de computador es cambiar una línea del `.env`, nada más.

**Los nombres de hoja cambian con el año** (`OT26` pasa a ser `OT27`). Se resuelven con una función que recibe el año. No se escriben como constantes en ninguna parte.

## 3. Estructura de los módulos

| Módulo | Qué hace |
|---|---|
| `core/config.py` | Lee `.env` y valida que exista lo que hace falta. Único lugar que toca el entorno |
| `etl/extract/google_sheets.py` | Abre el Sheet con la service account y entrega una hoja como texto |
| `etl/extract/excel_hdv.py` | Abre el `.xlsm` de la ruta configurada y entrega las hojas de vida |
| `etl/transform/normalizar.py` | Fechas, series, claves y texto. Se escribe con pruebas antes que nada |
| `etl/transform/<fuente>.py` | Una transformación por planilla |
| `etl/contracts/` | Un contrato pydantic por tabla de destino |
| `etl/load/` | Carga a la base, idempotente, con registro de la corrida |
| `scripts/` | Los comandos que se ejecutan a mano |

Regla de dependencia: `load` no sabe de dónde vinieron los datos, y `extract` no sabe a qué tabla van. Esto importa porque la fuente se va a mover de Google Sheets a OneDrive: ese día se reescribe `extract` y no se toca nada más.

### Cómo se comporta la configuración

Si falta `EXCEL_HOJA_DE_VIDA_PATH`, o la ruta no existe, o no está el archivo de credenciales, el ETL **se detiene antes de empezar** y dice qué variable está mal. Nunca sigue con una fuente vacía: una corrida que no encuentra la planilla tiene que fallar de forma ruidosa, no cargar cero filas en silencio.

### Cómo se leen las hojas de vida

El `.xlsm` tiene **963 hojas**: una por equipo, nombrada con el NIC o la serie, más la hoja `EQUIPOS CRITICOS 2019` que hace de índice con `EQUIPO`, `NIC` y `SERIE`.

Se lee el índice primero y solo después se abren las hojas que el índice nombra. El archivo se abre en modo solo lectura, que no bloquea a quien lo esté editando. Si la lectura falla porque alguien está guardando en ese momento, se reintenta; si vuelve a fallar, se copia el archivo a un temporal, se lee la copia, y queda anotado en el log que se leyó una copia y de qué hora.

Dentro de cada hoja el encabezado no está siempre en la misma fila: aparece entre la quinta y la undécima. Se busca la fila que tenga `FECHA` y las columnas `MC` y `MP`, que son las que indican si la intervención fue correctiva o preventiva.

## 4. Qué se rescata del prototipo

Los scripts de `legacy/scripts/a_data_import/` no se ejecutan ni se copian. Se usan como referencia y se reimplementan con pruebas.

| Del prototipo | Qué se hace |
|---|---|
| `convertir_fecha_estandar` | Se reimplementa, corrigiendo dos defectos: borra letras en silencio, y convierte lo irreconocible en fecha vacía sin dejar rastro. La versión nueva reporta el fallo como `FECHA_INVALIDA` |
| `dividir_y_agregar` | Se rescata la regla de separadores (espacio, `:`, `/`, `//`), corrigiendo que hoy solo aplica el primero que encuentra |
| `es_nic_valido` | Se rescata igual. Los bloqueos `APA-`, `AP-`, `ASSET` y `COD` son conocimiento de la unidad |
| `estandarizar_clave` | Se rescata partida en dos: para emparejar NIC con hojas se quitan los espacios; para la serie solo se recorta y se pasa a mayúsculas |
| `procesar_hoja_mantenimiento` | Se rescata completa. Es la parte más valiosa: describe cómo están armadas de verdad las hojas de vida |
| `google_sheet_integration` | Se rescata la mecánica de conexión. No su lista de hojas, que incluye una que no existe, ni su manejo de errores, que devuelve vacío cuando algo falla |
| `id_unico` | No se usa. Lo reemplaza la restricción de unicidad de `hoja_de_vida` |
| El filtrado contra el catastro | No se usa. El universo lo define el PMP, y lo que no cuadra se reporta |

## 5. Los catálogos se cargan de las planillas

**No hay semillas escritas a mano.** `database/seeds/` queda sin uso: los catálogos se cargan desde las planillas como cualquier otra fuente, y volver a cargarlos no duplica nada. Un solo mecanismo para todo.

**`tipo_equipo` sale de la hoja `INDICES Y COSTOS`**, que trae 352 tipos con las columnas `EQUIPO`, `FUNCIÓN`, `MANTENIMIENTO`, `RIESGO FÍSICO`, `ANTECEDENTES` e `IM`. La categoría se deduce del índice, según la regla del glosario:

| IM | Categoría |
|---|---|
| 22 | `CRITICO` |
| 19 | `RELEVANTE` |
| 12 a 18 | `IM_MAYOR_12` |

Los tipos con índice bajo 12, y los 168 que están sin evaluar, no entran: no tienen plan de mantenimiento y por lo tanto no tienen categoría. Un equipo cuyo tipo no esté cargado se reporta con motivo `TIPO_DESCONOCIDO`.

Hay un nombre repetido en la hoja, `Mesa Qx Avanzada`. Como el nombre del tipo es único en la base, la repetición se reporta y se carga una sola vez.

**`servicio_clinico` sale de dos hojas**: `Datos_Unidades` da la lista de unidades del hospital, y `Agenda` aporta el contacto de cada una — correo, anexo y responsable. La `Agenda` tiene varias personas por unidad, así que se toma como responsable la que tenga jefatura en el cargo; si no hay ninguna, la unidad queda cargada sin contacto.

`CLAVES` no entra acá: son claves de acceso de los equipos, no contactos de unidades.

## 6. Cambios en la base de datos

**Sin Alembic.** El esquema está escrito a mano en SQL y así se queda.

- `database/schema/` es el estado actual de la base. **No se edita nunca más.**
- Todo cambio posterior va como un archivo numerado en `database/migrations/` (`001_...sql`, `002_...sql`), que solo agrega, nunca reescribe.
- `scripts/aplicar_migraciones.py` aplica las que falten y anota cuáles ya aplicó en una tabla de la propia base.

Así, poner al día el servidor del hospital es correr un comando, y saber en qué estado está una base es mirar una tabla.

## 7. En qué orden se construye

| # | Corte | De dónde sale | A qué tabla llega |
|---|---|---|---|
| 0 | Base | — | `core/config.py`, `normalizar.py` con pruebas, registro de la corrida |
| 1 | Catálogos | `INDICES Y COSTOS`, `Datos_Unidades`, `Agenda` | `tipo_equipo`, `servicio_clinico` |
| 2 | Universo | `PMP`, `PMP IM>12`, `CATASTRO` | `equipo`, `plan_mantenimiento` |
| 3 | Hojas de vida | `HOJAS_DE_VIDA.xlsm`, `HDV ECER`, `HDV IM≥12` | `hoja_de_vida` |
| 4 | Órdenes de trabajo | `OT26` | `orden_trabajo` |
| 5 | Documentos | `AE`, `AP`, `CS`, `IT`, `AC`, `GD` | `hoja_de_vida` |
| 6 | Fallas | `GESTION DE FALLAS` | `falla` |

El corte 2 es el delicado: decide qué equipos existen. Su resultado correcto ya está medido, así que sirve de prueba — **781 equipos para 2026** (485 del PMP de críticos y relevantes, 296 del PMP de IM>12), 31 series con plan que no están en el catastro, y 24 que figuran como `NO APLICA`, todas reportadas. Si la primera corrida no da esos números, el error está en la transformación.

## 8. Cómo se trabaja cada corte

1. Rama `fase-2/<nombre-del-corte>`.
2. Pruebas de la transformación primero, con una muestra chica de datos reales.
3. Carga en la base local.
4. Revisión en psql de las tablas `carga` y `rechazo`: cuántas filas entraron, cuántas se rechazaron y por qué.
5. Commit en español.

Las pruebas que tocan Google o la base van marcadas con `@pytest.mark.integration`. Para `psql` se usa la herramienta PowerShell, no Bash.

## 9. Lo que no se negocia

- El universo de equipos lo define el PMP del año en curso, no el catastro.
- Lo que no cuadra se reporta en `rechazo`. No se borra ni se corrige por cuenta propia.
- Las planillas no se tocan: el ETL se adapta a los datos como vienen.
- Cada corrida abre una fila en `carga` y declara su origen antes de escribir, para que el registro de cambios distinga lo que hizo el ETL de lo que hizo una persona.
- Una fuente, una transacción: si algo falla a la mitad, no queda media planilla cargada.
