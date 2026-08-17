# Orientación Proyecto HRC-CEMS 🏥
## Web de Ingeniería Clínica - Hospital Copiapó

---

## 📊 Estado Actual

Tu proyecto está en **fase de desarrollo intermedia**. Tienes:
- ✅ Sistema de recolección de datos desde Google Sheets y Excel
- ✅ Normalización y procesamiento de datos (1,483 líneas de código)
- ✅ Análisis y clustering de datos
- ⏳ Visualización en desarrollo
- ❌ Base de datos SQL no implementada
- ❌ Web Flask incompleta
- ❌ Despliegue LAN sin configurar

---

## 🗂️ Estructura del Proyecto

```
HRC-CEMS/
├── scripts/
│   ├── a_data_import/          ← RECOLECCIÓN (376 líneas procesamiento)
│   │   ├── processing_raw_google_data.py    (Google Sheets)
│   │   ├── processing_raw_excel_hdv.py      (Excel)
│   │   ├── google_sheet_integration.py      (API integración)
│   │   └── utils.py                         (Funciones auxiliares)
│   │
│   ├── b_data_analytics/       ← PROCESAMIENTO & ANÁLISIS (650 líneas)
│   │   ├── analytics.py                     (Análisis principal)
│   │   ├── clustering_ot.py                 (Clusters HDP)
│   │   ├── clustering_problemas_eemm.py     (Clustering problemas)
│   │   └── visualizar_clusters_*.py         (Gráficos matplotlib)
│   │
│   ├── c_database_setup/       ← BASE DE DATOS (SIN IMPLEMENTAR)
│   │   └── setup_db.py                      (Vacío - SQL schema)
│   │
│   └── d_borradores/           ← Archivos temporales
│
├── data/
│   ├── raw/
│   │   ├── google_sheets/      (Datos descargados Google Sheets)
│   │   └── excel/              (Datos descargados Excel)
│   ├── processed/              (CSVs procesados)
│   ├── analytics/              (Resultados de análisis)
│   ├── clusters/               (Clusterización)
│   └── database/               (SQLite - sin usar)
│
├── requirements.txt            ← Dependencias
└── README.md                   ← Documentación

```

---

## 🔄 Flujo de Trabajo Actual

```
1️⃣ RECOLECTAR
   ├─ Google Sheets → processing_raw_google_data.py
   ├─ Excel → processing_raw_excel_hdv.py
   └─ Salida: /data/raw/ (CSVs)

2️⃣ PROCESAR & NORMALIZAR
   ├─ Convertir fechas (convertir_fecha_estandar)
   ├─ Limpiar datos (dividir_y_agregar)
   ├─ Integrar múltiples fuentes
   └─ Salida: /data/processed/ (CSVs normalizados)

3️⃣ ANALIZAR
   ├─ Estadísticas descriptivas
   ├─ Clusterización (HDP, K-means)
   └─ Salida: /data/analytics/ + /data/clusters/

4️⃣ VISUALIZAR
   ├─ Matplotlib (clusters actuales)
   └─ [INCOMPLETO] Flask web + templates
```

---

## 💾 Dependencias Instaladas

- **Data**: pandas, numpy, scipy
- **API**: gspread, google-auth (Google Sheets)
- **Web**: Flask 2.3.3 (instalado pero no usado)
- **ML**: transformers, torch (para análisis text embeddings)
- **Visualización**: matplotlib
- **Utils**: requests, beautifulsoup4, python-dotenv

**Nota**: requirements.txt tiene `torch` pero no se usa en scripts actuales → revisar dependencias reales.

---

## ⚠️ Problemas Identificados

### 1. **Carpetas Duplicadas de Scripts**
```
scripts/1. data_import/    ← Vieja numeración
scripts/a_data_import/     ← Nueva numeración (USAR ESTA)
```
**Acción**: Eliminar `scripts/1. data_import/` para evitar confusión.

### 2. **setup_db.py Vacío**
- Base de datos SQL no iniciada
- No hay schema definido
- No hay migrations planeadas

### 3. **Flask Instalado pero No Usado**
- No hay app.py o servidor web
- No hay templates HTML
- No hay rutas API definidas

### 4. **Dependencias Innecesarias**
- `torch` y `transformers` en requirements pero no utilizados
- `beautifulsoup4` sin uso aparente
- Limpiar requirements.txt

### 5. **Sin Integración Database-Web**
- Datos en CSVs, no en BD
- Sin API que consulte base de datos
- Sin caché de datos procesados

---

## ✅ Lo Que Funciona Bien

1. **Sistema de extracción robusto**
   - Manejo de múltiples formatos de fecha
   - Validación y logging estandarizado
   - Integración Google Sheets con API

2. **Normalización de datos**
   - Funciones reutilizables en utils.py
   - Procesamiento modular por fuente

3. **Análisis exploratorio**
   - Clusterización funcional
   - Visualizaciones básicas
   - Manejo de datos desorganizados del hospital

---

## 🚀 Próximas Etapas (Prioridad)

### FASE 1: Consolidar Backend (2-3 semanas)
1. **Limpiar estructura**
   - [ ] Eliminar carpetas `1. data_import` y `d_borradores`
   - [ ] Audit de dependencies (eliminar torch, beautifulsoup si no se usan)
   - [ ] Crear archivo `.env` para credenciales Google

2. **Implementar Base de Datos**
   - [ ] Diseñar schema SQL (equipos, mantenimientos, problemas reportados)
   - [ ] setup_db.py: crear tablas + índices
   - [ ] Script de migración: CSV → SQLite/PostgreSQL

3. **Automatizar recolección**
   - [ ] Scheduler para descargar datos cada X horas
   - [ ] Validar y guardar automáticamente en BD

### FASE 2: Crear API Web (2-3 semanas)
1. **Backend Flask**
   - [ ] app.py con rutas REST
   - [ ] Conexión a base de datos
   - [ ] Endpoints: /api/equipment, /api/maintenance, /api/analytics

2. **Frontend HTML/CSS**
   - [ ] Dashboard principal con gráficos
   - [ ] Tabla de equipos con filtros
   - [ ] Reportes de mantenimiento
   - [ ] Alertas de equipos con problemas

### FASE 3: Configurar Despliegue LAN (1-2 semanas)
1. [ ] Configurar Flask para LAN local
2. [ ] Testear conexión desde otros PCs del hospital
3. [ ] Documentación de instalación en servidor

---

## 📋 Recomendaciones Técnicas

### Estructura de Carpetas Mejorada (propuesta)
```
HRC-CEMS/
├── backend/
│   ├── app.py                    (Flask app principal)
│   ├── config.py                 (Configuración env)
│   ├── database/
│   │   ├── __init__.py
│   │   ├── models.py             (SQLAlchemy models)
│   │   ├── schema.sql            (SQL puro)
│   │   └── migrations.py
│   ├── data_pipeline/
│   │   ├── collectors/           (Google Sheets, Excel)
│   │   ├── processors/           (Limpieza y normalización)
│   │   ├── analytics/            (Clustering, análisis)
│   │   └── scheduler.py          (Cron automático)
│   └── routes/
│       ├── equipment.py
│       ├── maintenance.py
│       └── analytics.py
├── frontend/
│   ├── templates/
│   │   ├── base.html
│   │   ├── dashboard.html
│   │   ├── equipment.html
│   │   └── reports.html
│   └── static/
│       ├── css/
│       ├── js/
│       └── images/
├── data/                          (Datos sin versionear)
├── tests/                         (Unit tests)
├── docs/                          (Documentación técnica)
├── requirements.txt
├── .env.example
└── README.md
```

### Stack Recomendado
- **Backend**: Flask + SQLAlchemy (ORM)
- **Base de Datos**: SQLite (desarrollo/LAN simple) o PostgreSQL (escalable)
- **Frontend**: HTML5 + Bootstrap 5 + Chart.js (gráficos interactivos)
- **Servidor LAN**: Gunicorn + Nginx (opcional pero robusto)

---

## 🎯 Punto de Partida Inmediato

1. **Revisar y limpiar** `setup_db.py`
   - Define schema SQL para: Equipos, Mantenimientos, Problemas
   - Crea función `init_database()` que genere las tablas

2. **Crear** `backend/app.py` básico
   ```python
   from flask import Flask, render_template
   
   app = Flask(__name__)
   
   @app.route('/')
   def dashboard():
       return render_template('dashboard.html')
   
   if __name__ == '__main__':
       app.run(host='0.0.0.0', port=5000, debug=True)
   ```

3. **Crear** tabla simple en base de datos para equipos

4. **Conectar** un endpoint que lea datos y los devuelva en JSON

---

## 📞 Preguntas de Clarificación

Para ayudarte mejor, necesito saber:

1. ¿Qué datos específicos debe mostrar la web?
   - Inventario de equipos médicos
   - Historial de mantenimientos
   - Problemas/fallas reportadas
   - Análisis de clustering (equipos similares)
   - ¿Otro?

2. ¿Cuántos usuarios accederán simultáneamente?
   - <10 (SQLite es suficiente)
   - 10-50 (PostgreSQL recomendado)
   - >50 (Arquitectura escalable)

3. ¿Frecuencia de actualización de datos?
   - Manual (cuando necesites)
   - Diaria
   - En tiempo real

4. ¿Qué reportes son críticos?
   - Estado de equipos
   - Mantenimiento preventivo
   - Problemas por categoría
   - Análisis de tendencias

---

**Última actualización**: 2026-08-16  
**Estado del repositorio**: Activo en GitHub ✅
