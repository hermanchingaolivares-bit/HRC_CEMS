# Plan de la fase 2: el ETL

Este documento define **una sola forma de hacer cada cosa**. No propone alternativas: si más adelante hay que cambiar algo, se cambia aquí y se cambia entero.

Es el marco. El estado del código está en `CLAUDE.md`, y cómo ejecutarlo, en `COMO_EJECUTAR.md`.

## Las decisiones que cierran la fase

| Decisión | Qué significa |
|---|---|
| **Google siempre por la API** | Con la service account, tomando los valores como texto. Nunca desde un archivo exportado: la exportación corrompe las series numéricas largas |
| **El Excel, siempre el original** | La ruta se lee de `EXCEL_HOJA_DE_VIDA_PATH` y cambia según el computador: copia local en desarrollo, archivo de la unidad de red en el servidor. El código no distingue |
| **Capas separadas** | La carga no sabe de dónde vinieron los datos y la extracción no sabe a qué tabla van. Cuando la fuente se mueva a OneDrive, se reescribe solo la extracción |
| **Sin Alembic** | Los cambios de esquema son archivos numerados en `database/migrations/` que solo agregan. `database/schema/` no se edita nunca |
| **Sin semillas escritas a mano** | Los catálogos se cargan desde las planillas como cualquier otra fuente. `database/seeds/` queda sin uso |
| **Un solo entorno** | El conda `cems`, con Python 3.13, que incluye hasta Spyder. `requirements.txt` es la única lista de dependencias |

## En qué orden se construye

| # | Corte | De dónde sale | A qué tabla llega | Estado |
|---|---|---|---|---|
| 0 | Base | — | Configuración, normalización con pruebas, registro de cada corrida | ✅ |
| 1 | Catálogos | `INDICES Y COSTOS`, `Datos_Unidades`, `Agenda` | `tipo_equipo`, `servicio_clinico` | ✅ |
| 2 | Universo | `PMP`, `PMP IM>12`, `CATASTRO` | `equipo`, `plan_mantenimiento` | ⬜ |
| 3 | Hojas de vida | `HOJAS_DE_VIDA.xlsm`, `HDV ECER`, `HDV IM≥12` | `hoja_de_vida` | ⬜ |
| 4 | Órdenes de trabajo | `OT26` | `orden_trabajo` | ⬜ |
| 5 | Documentos | `AE`, `AP`, `CS`, `IT`, `AC`, `GD` | `hoja_de_vida` | ⬜ |
| 6 | Fallas | `GESTION DE FALLAS` | `falla` | ⬜ |

Cada corte llega hasta la base y se verifica ahí antes de empezar el siguiente.

**El corte 2 tiene su resultado medido de antemano**, así que sirve de prueba: 781 equipos para 2026 —485 del plan de críticos y relevantes, 296 del de IM>12—, 31 series con plan que no están en el catastro y 24 que figuran como `NO APLICA`, todas reportadas. Si la primera corrida no da esos números, el error está en la transformación, no en los datos.

## Cómo se trabaja cada corte

1. Rama propia en el repositorio.
2. Pruebas de la transformación primero, con una muestra chica de datos reales.
3. Carga en la base local.
4. Revisión de `carga` y `rechazo`: cuántas filas entraron, cuántas se rechazaron y por qué.
5. Commit en español.

## Qué se rescata del prototipo

Los scripts de `legacy/` no se ejecutan ni se copian: aportan las reglas del dominio y se reimplementan con pruebas. Lo que vale la pena rescatar en los cortes que faltan:

| Del prototipo | Para qué sirve |
|---|---|
| `procesar_hoja_mantenimiento` | Describe cómo están armadas de verdad las hojas de vida: el encabezado cae entre la quinta y la undécima fila, y las columnas `MC`/`MP` distinguen correctivo de preventivo. Es lo más valioso que hay ahí — corte 3 |
| `es_nic_valido` | Los bloqueos `APA-`, `AP-`, `ASSET`, `COD` son conocimiento de la unidad que no se deduce mirando los datos |
| `dividir_y_agregar` | La regla de separadores de series, ya reimplementada en `normalizar.py` |

Su lista de hojas no es de fiar: incluye `AMFE EQUIPOS`, que no existe. Y su manejo de errores devuelve vacío cuando algo falla, que es como una hoja inexistente pasó años sin que nadie lo notara.

## Lo que no se negocia

- El universo de equipos lo define el `PMP` del año en curso, no el catastro.
- Lo que no cuadra se reporta en `rechazo`. No se borra ni se corrige por cuenta propia.
- **Ignorado no es rechazado**: una fila que legítimamente no entra —un tipo sin plan, una fila en blanco— se cuenta aparte y no ensucia la lista de correcciones pendientes de la unidad.
- Las planillas no se tocan: el ETL se adapta a los datos como vienen.
- Cada corrida abre su fila en `carga` y declara su origen antes de escribir, para que el historial distinga al ETL de una edición manual.
- Una fuente, una transacción: si algo falla a la mitad, no queda media planilla cargada.
- Volver a correr una carga no duplica filas ni inventa historial.
