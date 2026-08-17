# Guía: SQLite vs PostgreSQL vs MySQL para tu Proyecto 🗄️

---

## ⚠️ Por Qué SQLite NO Escala

### SQLite = Archivo simple
```
SQLite = Un archivo .db en tu disco duro
         ↓
         Cuando 5+ usuarios accesan simultáneamente
         ↓
         El archivo se bloquea (1 escritura por vez)
         ↓
         Los demás usuarios esperan → LENTITUD
```

### Limitaciones de SQLite
| Problema | Impacto |
|----------|--------|
| **1 escritura a la vez** | Los usuarios no pueden enviar datos simultáneamente |
| **Sin autenticación real** | Cualquiera puede acceder (no seguro en LAN) |
| **Sin respaldos automáticos** | Si falla el archivo, pierdes todo |
| **Sin replicación** | No hay redundancia si falla el PC servidor |
| **Conexiones limitadas** | ~5-10 conexiones máximo antes de errores |

### Ejemplo Real en tu Hospital
```
Escenario SQLite:
- 3 ingeniero clínicos ingresando datos de equipos → ¡CRASH!
- Los primeros 2 escriben rápido, el 3º espera 5-10 segundos
- Si hay error en escritura, se puede corromper todo

Escenario PostgreSQL:
- 3 ingenieros escriben simultáneamente sin problemas
- Cada uno tiene su conexión independiente
- El servidor maneja todo automáticamente
```

---

## 🆓 Alternativas SQL Gratuitas & Open Source

### 1. **PostgreSQL** ⭐ RECOMENDADO PARA TI

**¿Qué es?**
- Base de datos profesional, empresa-grade, completamente gratis
- Lo usan: Spotify, Instagram, Uber (versiones iniciales)
- Open source (código abierto, puedes modificar)

**Ventajas:**
```
✅ Maneja 100+ conexiones simultáneas fácilmente
✅ Transacciones ACID (datos seguros incluso con errores)
✅ Soporte nativo para usuarios/permisos
✅ Respaldos automáticos configurables
✅ Replicación (copias de respaldo sincronizadas)
✅ Excelente para análisis (tu clustering futuro)
✅ Funciona perfectamente en LAN local
```

**Desventajas:**
```
❌ Necesita servidor separado (puede ser el mismo PC)
❌ Consume ~100-200 MB RAM (vs SQLite que consume nada)
❌ Requiere configuración inicial (30 min)
```

**Precio**: Gratis para siempre ✅

---

### 2. **MySQL / MariaDB** (Alternativa válida)

**¿Qué es?**
- Parecido a PostgreSQL
- MariaDB = "MySQL mejorado" (fork oficial de MySQL)
- Lo usan: Facebook, Airbnb, Slack

**Ventajas:**
```
✅ Similar a PostgreSQL en capacidad
✅ Un poco más rápido para lectura (pero MariaDB mejora esto)
✅ Muy popular en hosting compartido
✅ Excelente documentación
```

**Desventajas vs PostgreSQL:**
```
❌ Menos seguridad en transacciones
❌ No tan bueno para análisis complejos
❌ Menos eficiente con datos muy grandes
```

**Precio**: Gratis para siempre ✅

---

### 3. **SQL Server Express** (Microsoft)

**¿Qué es?**
- Versión "mini" del SQL Server empresarial
- Gratis para proyectos pequeños

**Desventajas:**
```
❌ Solo Windows
❌ Límite de 10 GB (después tienes que pagar)
❌ No es open source
❌ Más complicado de configurar en LAN
```

**No recomendado** para tu caso.

---

## 📊 Comparación Rápida

| Característica | SQLite | PostgreSQL | MySQL/MariaDB |
|---|---|---|---|
| **Usuarios simultáneos** | 1-5 | 50-100+ | 50-100+ |
| **Tamaño máximo** | 140 TB (teórico) | Ilimitado | Ilimitado |
| **Precio** | Gratis | Gratis | Gratis |
| **Seguridad** | Nula | Excelente | Buena |
| **Backup automático** | No | Sí | Sí |
| **Mejor para análisis** | No | **SÍ** | No |
| **Facilidad setup** | 5 min | 30 min | 20 min |
| **Para LAN hospital** | ❌ | ✅✅✅ | ✅✅ |

---

## 🚀 RECOMENDACIÓN PARA TI: PostgreSQL

### Por qué PostgreSQL es mejor para tu hospital:
1. **Escalable**: De 2 a 50+ usuarios sin cambiar código
2. **Seguro**: Usuarios, permisos, autenticación nativa
3. **Análisis**: Perfect para tu clustering futuro (mejor soporte JSON, arrays)
4. **Gratis**: Genuinamente gratis, sin limitaciones ocultas
5. **LAN-friendly**: Se configura fácilmente en red local

---

## ⚙️ CÓMO CAMBIAR TU CÓDIGO: SQLite → PostgreSQL

### Paso 1: Instalar PostgreSQL

**En Windows:**
1. Descargar: https://www.postgresql.org/download/windows/
2. Ejecutar instalador
3. Recordar la contraseña para usuario `postgres`
4. Instalar pgAdmin (herramienta visual incluida)

**En Ubuntu/Mac:**
```bash
# Ubuntu
sudo apt-get install postgresql postgresql-contrib

# Mac (homebrew)
brew install postgresql
```

### Paso 2: Crear Base de Datos

**Opción A: Con pgAdmin (interfaz gráfica)**
1. Abrir pgAdmin
2. Right-click → Create → Database
3. Nombre: `hrc_clinical_engineering`
4. Crear

**Opción B: Con terminal**
```bash
sudo -u postgres psql
CREATE DATABASE hrc_clinical_engineering;
CREATE USER hrc_user WITH PASSWORD 'tu_contraseña_segura';
ALTER ROLE hrc_user SET client_encoding TO 'utf8';
GRANT ALL PRIVILEGES ON DATABASE hrc_clinical_engineering TO hrc_user;
\q
```

### Paso 3: Modificar requirements.txt

**Elimina esto:**
```
# Dependencias viejas innecesarias
torch==2.0.1
transformers==4.30.2
beautifulsoup4==4.12.2
```

**Agrega esto:**
```
psycopg2-binary==2.9.9  # Conector PostgreSQL para Python
SQLAlchemy==2.0.21      # ORM (abstracción BD)
alembic==1.12.0         # Migrations (gestionar cambios de schema)
python-dotenv==1.0.0    # Variables de entorno (credenciales)
```

**Instalar:**
```bash
pip install -r requirements.txt
```

### Paso 4: Crear archivo `.env` (credenciales seguras)

```bash
# .env (NO SUBIR A GITHUB!)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hrc_clinical_engineering
DB_USER=hrc_user
DB_PASSWORD=tu_contraseña_segura
```

**Actualizar `.gitignore`:**
```
.env
__pycache__/
*.pyc
*.db
.DS_Store
```

### Paso 5: Crear `config.py` (conexión a BD)

```python
# scripts/config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuración para conexión a base de datos"""
    
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', 5432)
    DB_NAME = os.getenv('DB_NAME', 'hrc_clinical_engineering')
    DB_USER = os.getenv('DB_USER', 'hrc_user')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    
    # String de conexión para SQLAlchemy
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # Para conexión directa con psycopg2
    DB_CONFIG = {
        'host': DB_HOST,
        'port': DB_PORT,
        'database': DB_NAME,
        'user': DB_USER,
        'password': DB_PASSWORD
    }

# Uso en tus scripts:
# from config import Config
# conexion = psycopg2.connect(**Config.DB_CONFIG)
```

### Paso 6: Crear `setup_db.py` NUEVO (ahora con PostgreSQL)

```python
# scripts/c_database_setup/setup_db.py
import psycopg2
from psycopg2.extras import execute_values
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database():
    """Crear schema y tablas en PostgreSQL"""
    
    try:
        conn = psycopg2.connect(**Config.DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. TABLA: EQUIPOS MÉDICOS
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS equipos (
                id_equipo SERIAL PRIMARY KEY,
                nombre VARCHAR(150) NOT NULL,
                codigo_interno VARCHAR(50) UNIQUE NOT NULL,
                tipo VARCHAR(100),
                marca VARCHAR(100),
                modelo VARCHAR(100),
                serial VARCHAR(100),
                fecha_adquisicion DATE,
                ubicacion VARCHAR(150),
                estado VARCHAR(20) DEFAULT 'OPERATIVO',
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("✅ Tabla 'equipos' creada")
        
        # 2. TABLA: MANTENIMIENTOS
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mantenimientos (
                id_mantenimiento SERIAL PRIMARY KEY,
                id_equipo INTEGER NOT NULL,
                tipo_mantenimiento VARCHAR(50),
                descripcion TEXT,
                fecha_realizado DATE,
                fecha_proximo DATE,
                responsable VARCHAR(100),
                estado_equipo_pre VARCHAR(50),
                estado_equipo_post VARCHAR(50),
                costo DECIMAL(10,2),
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_equipo) REFERENCES equipos(id_equipo)
            )
        """)
        logger.info("✅ Tabla 'mantenimientos' creada")
        
        # 3. TABLA: PROBLEMAS/FALLAS
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS problemas (
                id_problema SERIAL PRIMARY KEY,
                id_equipo INTEGER NOT NULL,
                descripcion TEXT NOT NULL,
                severidad VARCHAR(20),
                fecha_reporte DATE,
                fecha_resolucion DATE,
                responsable VARCHAR(100),
                estado VARCHAR(20) DEFAULT 'ABIERTO',
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_equipo) REFERENCES equipos(id_equipo)
            )
        """)
        logger.info("✅ Tabla 'problemas' creada")
        
        # 4. CREAR ÍNDICES (para búsquedas rápidas)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_equipos_tipo ON equipos(tipo)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_equipos_estado ON equipos(estado)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mantenimientos_equipo ON mantenimientos(id_equipo)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_problemas_equipo ON problemas(id_equipo)")
        logger.info("✅ Índices creados")
        
        conn.commit()
        logger.info("✅ BASE DE DATOS INICIALIZADA CORRECTAMENTE")
        
    except Exception as e:
        logger.error(f"❌ Error inicializando base de datos: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    init_database()
```

**Ejecutar:**
```bash
cd scripts/c_database_setup
python setup_db.py
```

### Paso 7: Modificar tu script de procesamiento

**Antes (guardaba en CSV):**
```python
def save_csv(df, name):
    path = os.path.join(processed_dir, name)
    df.to_csv(path, index=False, encoding='utf-8')
```

**Después (guarda en PostgreSQL):**
```python
import psycopg2
from psycopg2.extras import execute_values
from config import Config

def save_to_database(df, table_name):
    """Guardar DataFrame directamente a PostgreSQL"""
    try:
        conn = psycopg2.connect(**Config.DB_CONFIG)
        cursor = conn.cursor()
        
        # Convertir a tuplas
        values = [tuple(row) for row in df.values]
        columns = ",".join(df.columns)
        
        # INSERT multiple rows eficientemente
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
        execute_values(cursor, insert_query, values)
        
        conn.commit()
        logger.info(f"✅ {len(df)} filas guardadas en {table_name}")
        
    except Exception as e:
        logger.error(f"❌ Error guardando en BD: {e}")
    finally:
        cursor.close()
        conn.close()

# Uso:
# df_equipos = load_csv("equipos_raw.csv")
# save_to_database(df_equipos, "equipos")
```

---

## 🔄 PLAN DE MIGRACIÓN (Sin perder datos)

### Opción 1: Hacerlo bien (RECOMENDADO)
```
Día 1:
  ↓
1. Instalar PostgreSQL
2. Crear base de datos vacía
3. Crear setup_db.py nuevo
4. Ejecutar setup_db.py
5. Modificar scripts de procesamiento
6. Ejecutar procesamiento → datos van directo a PostgreSQL
7. Verificar datos en pgAdmin
8. ¡Listo!
```

### Opción 2: Migrar datos existentes de CSV → PostgreSQL
```python
# Script de migración
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from config import Config

def migrate_csv_to_db(csv_path, table_name):
    df = pd.read_csv(csv_path)
    conn = psycopg2.connect(**Config.DB_CONFIG)
    cursor = conn.cursor()
    
    values = [tuple(row) for row in df.values]
    columns = ",".join(df.columns)
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
    
    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()

# Uso:
# migrate_csv_to_db('data/processed/equipos.csv', 'equipos')
```

---

## 🎯 OTRAS COSAS QUE DEBES MODIFICAR

### 1. **requirements.txt LIMPIO**
```txt
# Data & Processing
pandas==2.0.3
numpy==1.24.3
scipy==1.11.1

# API Google Sheets
gspread==5.10.0
google-auth==2.17.3
google-auth-oauthlib==1.0.0
google-auth-httplib2==0.1.0

# Base de Datos PostgreSQL
psycopg2-binary==2.9.9
SQLAlchemy==2.0.21
alembic==1.12.0

# Web
Flask==2.3.3
Flask-SQLAlchemy==3.0.5  # Integración Flask + SQLAlchemy

# Utilities
openpyxl==3.1.2
requests==2.31.0
python-dotenv==1.0.0
matplotlib==3.7.2

# ❌ ELIMINAR:
# torch==2.0.1
# transformers==4.30.2
# beautifulsoup4==4.12.2
```

### 2. **Estructura de carpetas actualizada**

```
scripts/
├── config.py                          ← NUEVO: credenciales BD
├── a_data_import/
│   ├── __init__.py
│   ├── google_sheet_integration.py
│   ├── processing_raw_google_data.py  ← Modificar: guardar en BD
│   ├── processing_raw_excel_hdv.py    ← Modificar: guardar en BD
│   └── utils.py
├── b_data_analytics/
│   ├── __init__.py
│   ├── analytics.py                   ← Modificar: leer de BD
│   ├── clustering_ot.py
│   ├── clustering_problemas_eemm.py
│   └── visualizar_clusters_*.py
└── c_database_setup/
    └── setup_db.py                    ← NUEVO COMPLETO: crear schema PostgreSQL

# Eliminar:
# - scripts/1. data_import/
# - scripts/d_borradores/
```

### 3. **Archivo `.env.example` para el equipo**

```
# .env.example (SÍ SUBIR A GITHUB - como plantilla)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hrc_clinical_engineering
DB_USER=hrc_user
DB_PASSWORD=tu_contraseña_aqui

# Contribuidores copian esto a .env y rellenan con sus valores
```

### 4. **Crear archivo `SETUP_POSTGRESQ.md` (instrucciones para PC hospital)**

Documento que explique paso a paso cómo:
- Instalar PostgreSQL en el PC del hospital
- Configurar conexión
- Restaurar base de datos desde backup

---

## 📈 Escalabilidad: SQLite → PostgreSQL → PostgreSQL Remoto

```
Tu evolución probable:

Hoy (SQLite):                  Fase 2-3 (PostgreSQL Local):     Futuro (PostgreSQL Servidor)
┌─────────────┐               ┌──────────────────┐              ┌──────────────────────┐
│  SQLite     │               │  PC del Hospital │              │  Servidor dedicado   │
│  .db file   │               │  + PostgreSQL    │              │  + PostgreSQL 24/7   │
│             │               │  Acceso LAN      │              │  Acceso LAN/Internet │
│ Problema:   │               │                  │              │                      │
│ 5+ usuarios │ Upgrade →     │ Excelente para   │ Upgrade →   │ Acceso desde cualquier
│ = CRASH ❌  │               │ 20-30 usuarios ✅│             │ ubicación ✅         │
└─────────────┘               └──────────────────┘              └──────────────────────┘

CÓDIGO permanece casi IGUAL en todas las fases
Solo cambias: DATABASE_URL en config.py
```

---

## ✅ CHECKLIST DE CAMBIOS

```
FASE 1: MIGRACIÓN A POSTGRESQL (ESTA SEMANA)
─────────────────────────────────────────
[ ] Instalar PostgreSQL en tu PC
[ ] Crear base de datos "hrc_clinical_engineering"
[ ] Crear usuario "hrc_user"
[ ] Crear archivo .env con credenciales
[ ] Limpiar requirements.txt (eliminar torch, transformers, beautifulsoup)
[ ] Agregar psycopg2, SQLAlchemy, alembic a requirements.txt
[ ] Crear scripts/config.py
[ ] Crear setup_db.py NUEVO (código PostgreSQL)
[ ] Ejecutar setup_db.py para crear tablas
[ ] Verificar tablas en pgAdmin
[ ] Modificar processing_raw_google_data.py (guardar en BD)
[ ] Modificar processing_raw_excel_hdv.py (guardar en BD)
[ ] Ejecutar procesamiento → verificar datos en BD
[ ] Eliminar carpetas viejas (1. data_import, d_borradores)
[ ] Agregar .env a .gitignore
[ ] Crear .env.example
[ ] Crear SETUP_POSTGRESQL.md (guía para hospital)
[ ] Push a GitHub

FASE 2: INTEGRAR CON FLASK (PRÓXIMA SEMANA)
─────────────────────────────────────────
[ ] Crear backend/app.py que conecte con PostgreSQL
[ ] Crear rutas API: /api/equipos, /api/mantenimientos
[ ] Conectar Frontend a BD
```

---

**¿Tienes dudas? Cuéntame:**
1. ¿Ya instalaste PostgreSQL antes?
2. ¿Necesitas que explique cómo instalar PostgreSQL paso a paso?
3. ¿Quieres que ayude con la migración de código específico?
