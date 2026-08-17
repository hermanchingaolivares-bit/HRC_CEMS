# HRC-CEMS

Sistema de gestión de equipos médicos (EEMM) de la unidad de Ingeniería Clínica del **Hospital Regional de Copiapó**.

El objetivo es una aplicación web desplegada en la LAN del hospital que muestre el inventario de equipos, el estado de los mantenimientos y el análisis de fallas, alimentada por un ETL que consolida los datos hoy dispersos en Google Sheets y planillas Excel.

> **Estado: en construcción.** La estructura del proyecto está definida y el entorno acondicionado, pero los módulos (`etl/`, `app/`, `database/`) todavía no tienen implementación. Se irán escribiendo desde cero.

## Estructura

```
HRC-CEMS/
├── app/            Web Flask: rutas, servicios, modelos, plantillas, estáticos
├── etl/            Extracción, transformación, carga y contratos de datos
├── core/           Configuración y utilidades transversales
├── database/       Esquema SQL, migraciones y datos semilla
├── analytics/      Modelos de análisis y notebooks
├── data/           Datos de entrada y salida (contenido no versionado)
├── reports/        Figuras y exportaciones generadas (no versionado)
├── logs/           Registros de ejecución (no versionado)
├── tests/          Pruebas unitarias e integración
├── docs/           Documentación técnica
├── scripts/        Utilidades de línea de comandos
└── legacy/         Borrador anterior, archivado como referencia
```

`legacy/` contiene el prototipo previo: exploraciones de extracción, procesamiento y clustering, además de tres documentos de orientación. **No es código en producción ni está pensado para ejecutarse**; se conserva porque documenta el dominio y las reglas de negocio que hay que reimplementar.

## Puesta en marcha

Requiere **Python 3.11 o superior** (el entorno de desarrollo actual usa Anaconda con Python 3.13).

```bash
python -m venv .venv
```

```bash
.venv\Scripts\activate
```

```bash
pip install -r requirements.txt
```

Copiar la plantilla de variables de entorno y rellenarla:

```bash
copy .env.example .env
```

Además del `.env`, el ETL necesitará `secrets/credentials.json`: una service account de Google con acceso de lectura al Google Sheet `EEMM`. Ninguno de los dos se versiona, así que hay que crearlos en cada máquina.

## Desarrollo

Linter y formateador:

```bash
python -m ruff check .
```

```bash
python -m ruff format .
```

Pruebas:

```bash
python -m pytest
```

Una prueba concreta:

```bash
python -m pytest tests/unit/test_x.py::test_caso -v
```

La configuración de ambas herramientas está en `pyproject.toml`. `legacy/` queda excluido del linter a propósito.

## Documentación

- `CLAUDE.md`: guía del repositorio, flujo de datos previsto y glosario de siglas del dominio clínico.
- `legacy/ORIENTACION_PROYECTO.md`: diagnóstico del prototipo y hoja de ruta por fases.
- `legacy/ESTRUCTURA_PROYECTO.md`: arquitectura propuesta con plantillas de referencia.
- `legacy/GUIA_BASES_DATOS.md`: comparativa de motores SQL y pasos de migración.
