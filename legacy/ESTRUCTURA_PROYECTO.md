# Estructura Proyecto Web Ingeniería Clínica - HRC Copiapó

## Árbol de directorios

```
hrc-cems/
├── .github/
│   └── workflows/           # CI/CD (opcional por ahora)
│
├── src/
│   ├── app.py              # Punto de entrada principal
│   ├── config.py           # Variables de configuración (dev, prod)
│   ├── requirements.txt    # Dependencias Python
│   │
│   ├── data/               # MÓDULO: Ingesta y procesamiento de datos
│   │   ├── __init__.py
│   │   ├── collectors/     # Recopilar datos de fuentes
│   │   │   ├── __init__.py
│   │   │   └── base_collector.py      # Clase base para lectores
│   │   ├── processors/     # Normalizar y procesar
│   │   │   ├── __init__.py
│   │   │   └── data_processor.py      # Limpieza, normalización
│   │   └── loaders.py      # Cargar a BD
│   │
│   ├── models/             # MÓDULO: Modelos de datos (ORM)
│   │   ├── __init__.py
│   │   ├── base.py         # Clase base con timestamps
│   │   ├── equipment.py    # Equipos médicos
│   │   ├── maintenance.py  # Mantenimiento preventivo
│   │   ├── inspection.py   # Inspecciones técnicas
│   │   └── user.py         # Usuarios del sistema
│   │
│   ├── services/           # MÓDULO: Lógica de negocio
│   │   ├── __init__.py
│   │   ├── equipment_service.py       # Operaciones equipos
│   │   ├── maintenance_service.py     # Operaciones mantenimiento
│   │   └── report_service.py          # Generación de reportes
│   │
│   ├── routes/             # MÓDULO: Endpoints Flask (API/vistas)
│   │   ├── __init__.py
│   │   ├── equipment_routes.py
│   │   ├── maintenance_routes.py
│   │   ├── dashboard_routes.py
│   │   └── admin_routes.py
│   │
│   ├── templates/          # MÓDULO: HTML Jinja2
│   │   ├── base.html
│   │   ├── layout.html
│   │   ├── dashboard.html
│   │   ├── equipment/
│   │   │   ├── list.html
│   │   │   ├── detail.html
│   │   │   └── edit.html
│   │   └── maintenance/
│   │       ├── list.html
│   │       └── form.html
│   │
│   ├── static/             # CSS, JS, imágenes
│   │   ├── css/
│   │   │   ├── style.css
│   │   │   └── dashboard.css
│   │   ├── js/
│   │   │   ├── app.js
│   │   │   └── utils.js
│   │   └── img/
│   │
│   └── utils/              # Utilidades generales
│       ├── __init__.py
│       ├── db.py           # Inicialización BD
│       ├── decorators.py   # Decoradores (autenticación, etc)
│       └── helpers.py      # Funciones auxiliares
│
├── database/
│   ├── schema.sql          # Script creación esquema
│   ├── init_data.sql       # Datos iniciales
│   └── migrations/         # Versionado de cambios (futuro)
│
├── docs/
│   ├── README.md           # Guía general
│   ├── SETUP.md            # Configuración inicial
│   ├── API.md              # Documentación endpoints
│   └── DATA_FLOW.md        # Flujo de datos detallado
│
├── tests/                  # Tests unitarios e integración
│   ├── __init__.py
│   ├── conftest.py         # Fixtures pytest
│   ├── test_models.py
│   ├── test_services.py
│   └── test_routes.py
│
├── scripts/                # Scripts auxiliares
│   ├── init_db.py          # Crear BD desde cero
│   ├── backup.py           # Respaldo BD
│   └── etl_runner.py       # Ejecutar pipeline datos
│
├── .gitignore
├── README.md
└── setup.py                # Configuración para desarrollo


```

## Flujo de Datos (implementación)

```
RECOPILAR (collectors/)
    ↓
    Fuentes varias:
    - Archivos Excel/CSV
    - Bases datos legacy
    - Entrada manual
    ↓
NORMALIZAR (processors/)
    ↓
    - Validar tipos
    - Limpiar duplicados
    - Estandarizar formatos
    ↓
CARGAR (loaders.py + models/)
    ↓
    → Base datos (SQL Server / SQLite local)
    ↓
VISUALIZAR (routes/ + templates/)
    ↓
    - Dashboard
    - Listados
    - Reportes
```

## Componentes clave

### 1. **app.py** (punto de entrada)
```python
from flask import Flask
from config import Config
from utils.db import db, init_db

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Inicializar extensiones
    db.init_app(app)
    
    # Registrar blueprints (rutas modulares)
    from routes import equipment_bp, maintenance_bp, dashboard_bp
    app.register_blueprint(equipment_bp)
    app.register_blueprint(maintenance_bp)
    app.register_blueprint(dashboard_bp)
    
    with app.app_context():
        init_db()
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host='0.0.0.0')  # Accesible desde LAN
```

### 2. **Modelos** (SQLAlchemy ORM)
Cada tabla → una clase Python con columnas tipadas

### 3. **Servicios** (lógica)
Separar queries complejas, validaciones, cálculos → no en rutas directamente

### 4. **Rutas** (Flask blueprints)
Solo reciben parámetros → llaman servicio → retornan JSON o template

### 5. **ETL** (data/)
Pipeline independiente: puede ejecutarse:
- Manualmente (scripts/)
- Por scheduler (APScheduler)
- Desde interfaz admin

## Stack de dependencias (requirements.txt)

```
Flask==2.3.0
Flask-SQLAlchemy==3.0.0
python-dotenv==1.0.0
pandas==2.0.0          # Procesamiento datos
openpyxl==3.1.0        # Lectura Excel
requests==2.31.0       # HTTP requests
Werkzeug==2.3.0        # Seguridad
pytest==7.3.0          # Testing
```

## Configuración (config.py)

```python
import os
from datetime import timedelta

class Config:
    # Base
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-key-change-in-prod')
    
    # Base de datos
    SQLALCHEMY_DATABASE_URI = os.getenv(
        'DATABASE_URL', 
        'sqlite:///hrc-cems.db'  # Local dev
    )
    
    # Para SQL Server (producción):
    # 'mssql+pyodbc://user:password@servidor/db?driver=ODBC+Driver+17'
    
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = True  # Ver queries en desarrollo

class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False
```

## Ventajas de esta estructura

✅ **Modular**: cambios en BD no afectan rutas  
✅ **Testeable**: cada capa aislada  
✅ **Escalable**: fácil agregar nuevos módulos  
✅ **Limpio**: separación clara de responsabilidades  
✅ **Git-friendly**: commits por característica  
✅ **Colaborativo**: dos personas pueden trabajar en ramas diferentes sin conflictos  

## Pasos siguientes (orden recomendado)

1. **Setup inicial**: crear estructura, repositorio Git
2. **BD + Modelos**: definir tablas, ORM
3. **ETL básico**: collector → processor → loader (1-2 fuentes)
4. **Dashboard**: primeras vistas HTML
5. **Iteración**: agregar equipos, mantenimiento, reportes
6. **Testing**: tests para lo crítico
7. **Documentación**: mantener docs actualizadas

---

¿Comenzamos con el paso 1? Puedo ayudarte a:
- Crear estructura carpetas y archivos base
- Configurar Git y .gitignore
- Plantilla inicial de app.py y config.py
