# Cómo ejecutar el proyecto en Spyder

Guía práctica para correr el ETL desde Spyder, y dónde queda guardado cada dato.

## 1. El entorno del proyecto

Todo el proyecto vive en **un solo entorno de conda llamado `cems`**, con Python 3.13. Ahí adentro está todo: las librerías del ETL, las pruebas, el linter y el propio Spyder. No hay nada del proyecto instalado fuera de ese entorno.

Su intérprete es:

```
C:\Users\herma\anaconda3\envs\cems\python.exe
```

**Para abrir Spyder**, desde el *Anaconda Prompt*:

```bash
conda activate cems
```

y después:

```bash
spyder
```

Abierto así, Spyder ya usa el intérprete correcto y no hay que configurar nada. Si preferís abrirlo desde Anaconda Navigator, seleccioná primero el entorno `cems` en la lista de arriba.

**El proyecto**: *Proyectos → Abrir proyecto*, y elegir la carpeta `HRC-CEMS`. Con eso Spyder trabaja siempre desde la raíz y el explorador de archivos muestra el árbol completo.

### Crear el entorno en una máquina nueva

Es lo primero que hay que hacer en el PC del hospital. Son dos comandos en el *Anaconda Prompt*:

```bash
conda create -n cems python=3.13 -y
```

```bash
conda activate cems && pip install -r requirements.txt spyder
```

`requirements.txt` es la única lista de dependencias del proyecto: si falta algo, se agrega ahí y no se instala suelto. Después hay que recrear a mano el `.env` y `secrets/credentials.json`, que no se versionan.

Si por algún motivo Spyder termina usando otro intérprete, los scripts del proyecto lo detectan al arrancar y dicen cuál elegir. No hay que interpretar ningún error raro.

## 2. Lo primero que hay que correr

Abrir `scripts/verificar_entorno.py` y presionar **F5**.

Ese script no toca nada: revisa el `.env`, las credenciales de Google, el Excel de hojas de vida, la conexión con PostgreSQL y las librerías. Si algo falta, dice qué variable revisar y en qué archivo. Mientras no salga *"Todo listo"*, no tiene sentido correr nada más.

Los scripts se ubican solos: da lo mismo desde qué carpeta los ejecute Spyder.

## 3. Qué hace cada script

| Script | Qué hace | Escribe en |
|---|---|---|
| `scripts/verificar_entorno.py` | Revisa que la máquina pueda correr el ETL | Nada |
| `scripts/leer_hoja_google.py` | Lee una hoja del Sheet y muestra sus columnas | `data/raw/google_sheets/` |

En `leer_hoja_google.py` se cambia la hoja editando la variable `HOJA` arriba del archivo y se vuelve a presionar F5. No hay que escribir nada en la consola.

## 4. Dónde se guarda cada cosa

Esta es la parte que conviene tener clara antes de generar datos. **Ningún módulo arma rutas por su cuenta**: todas salen de `core/rutas.py`, así que si algo aparece guardado en otro lado, es un error.

| Carpeta | Qué contiene | Cuándo mirarla |
|---|---|---|
| `data/raw/` | Lo que se leyó, tal cual salió de la fuente | Para revisar un problema sin volver a consultar Google |
| `data/interim/` | Lo mismo ya normalizado, antes de decidir qué entra | Para ver qué hizo la limpieza |
| `data/processed/` | Lo que efectivamente se cargó a la base | Para comparar contra la base |
| `data/backups/` | Respaldos de la base de datos | Antes de un cambio grande |
| `logs/` | El registro de cada corrida, un archivo por corrida | Cuando algo falla |
| `reports/` | Reportes para la unidad | Al preparar un informe |

Tres reglas que evitan perder información:

- **Nada de eso se versiona.** Son datos del hospital: git conserva la estructura de carpetas, no el contenido. Al crear una carpeta nueva ahí dentro, `core/rutas.py` le deja su `.gitkeep` solo.
- **Cada etapa escribe en su propia carpeta.** El prototipo sobreescribía el mismo archivo en cada paso, así que cuando algo salía mal no había con qué comparar.
- **La copia en `data/raw/` no es una fuente.** Es evidencia de la corrida. El ETL siempre lee de Google por la API y del `.xlsm` original, nunca de esas copias.

Aparte quedan las dos fuentes verdaderas, que **no se tocan**:

- El Google Sheet `EEMM`, que se lee por la API con `secrets/credentials.json`.
- El Excel `HOJAS_DE_VIDA.xlsm`, en la ruta que diga `EXCEL_HOJA_DE_VIDA_PATH`.

## 5. La ruta del Excel cambia en cada computador

Es lo único que hay que ajustar al pasar de un PC a otro, y va en el `.env`:

- **En este PC (desarrollo):** una copia local en `data/raw/excel/`.
- **En el PC servidor del hospital:** el archivo **original** de la unidad de red, porque ahí la unidad lo abre y lo edita todos los días.

```
EXCEL_HOJA_DE_VIDA_PATH=Y:/PMP Y HOJAS DE VIDA/HOJAS_DE_VIDA (Autoguardado).xlsm
```

Una sola línea con esa variable: si aparece dos veces, manda la última y la otra se pierde en silencio.

El ETL abre ese archivo en **solo lectura**, así que no bloquea a quien lo esté usando. Si justo alguien está guardando, trabaja sobre una copia y lo anota en el registro con la hora del original, para saber a qué momento corresponden los datos.

## 6. Las pruebas

Las pruebas rápidas no tocan ni Google ni la base, y corren en menos de un segundo:

```bash
C:\Users\herma\anaconda3\envs\cems\python.exe -m pytest
```

Las que sí usan la base de datos van aparte, porque necesitan PostgreSQL corriendo:

```bash
C:\Users\herma\anaconda3\envs\cems\python.exe -m pytest -m integration -v
```

Esas crean filas de prueba y las borran al terminar: la base queda como estaba.

## 7. Errores frecuentes

| Lo que dice | Qué significa |
|---|---|
| `ErrorDeConfiguracion: Falta configuracion en ... .env` | Falta una variable. Copiar `.env.example` y completarla |
| `No se encontro el Excel de hojas de vida en ...` | La ruta del `.env` apunta a un archivo que no está en esta máquina |
| `No se pudo conectar con la base de datos` | PostgreSQL apagado, o `DATABASE_URL` mal escrita |
| `ModuleNotFoundError: No module named 'core'` | Se ejecutó un archivo suelto en vez de uno de `scripts/`. Los de `scripts/` se ubican solos |
| `ModuleNotFoundError: No module named 'pydantic_settings'` | Spyder está usando el intérprete equivocado. Ver el punto 1 |
| `Este proyecto necesita Python 3.11 o superior` | Lo mismo, pero ya detectado: el mensaje dice qué intérprete elegir |
| `Falta la libreria gspread` | Falta instalar dependencias: ver el punto 1 |

Cuando algo falla a mitad de una carga, la base **no queda a medias**: cada fuente se carga dentro de una sola transacción, así que o entra completa o no entra nada. El detalle de lo que pasó queda en `logs/`.
