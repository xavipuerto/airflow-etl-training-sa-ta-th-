#!/usr/bin/env python3
"""
Training DAG: ETL Architecture with SA-TA-TH Pattern
====================================================

This DAG demonstrates a complete ETL flow using public APIs

🎯 LEARNING OBJECTIVE:
Teach the SA-TA-TH layered architecture in an ETL process

📚 KEY CONCEPTS:
- SA (Staging Area): Landing zone with TRUNCATE+INSERT
- TA (Auxiliary Tables): For joins and high-volume transformations (not used in this simple example)
- TH (Historical Tables): Persistence layer with MERGE or INSERT append-only

🔄 WORKFLOW:
1. Get All Countries: API /all → SA + TH (Complete ETL with MERGE)
2. Get Regions Stats: API /region/{region} → Aggregation → SA + TH (with MERGE)
3. Get Air Quality: API /measurements → SA + TH (time series, INSERT append-only)

📊 DATA SOURCES:
- REST Countries API: https://restcountries.com/ (countries data)
- AQICN API: https://aqicn.org/api/ (air quality & weather)

💡 USE CASES:
- Master data with slow changes (countries) → MERGE
- Aggregations and statistics → MERGE
- Time series (air quality) → INSERT append-only (prepared for TimescaleDB)
- JOIN between different sources (countries + air quality)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')

# Import ETL functions (functional scripts) - Multiple SA tables
from training_get_countries_basic import etl_get_countries_basic
from training_get_countries_geo import etl_get_countries_geo
from training_get_countries_culture import etl_get_countries_culture
from training_merge_countries_to_th import etl_merge_countries_to_th
from training_get_regions_stats import etl_get_regions_stats
from training_get_weather import etl_get_weather_data
from training_get_air_quality_aqicn import etl_get_air_quality


# =============================================================================
# DAG CONFIGURATION
# =============================================================================

default_args = {
    'owner': 'training',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='training_etl_sa_ta_th',
    default_args=default_args,
    description='🎓 Training: ETL with SA-TA-TH architecture using REST Countries API',
    schedule=timedelta(minutes=5),  # Run every 5 minutes
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['training', 'etl', 'sa-ta-th', 'rest-countries'],
)


# =============================================================================
# WRAPPERS FOR AIRFLOW
# =============================================================================

def wrapper_get_countries_basic(**context):
    """
    Wrapper for ETL: Get Countries BASIC
    
    Executes loading of basic fields: API /all (8 fields) → SA basic
    """
    execution_id = context['run_id']
    print(f"🎯 Execution ID: {execution_id}")
    
    stats = etl_get_countries_basic(execution_id=execution_id)
    context['task_instance'].xcom_push(key='countries_basic_stats', value=stats)
    
    print(f"✅ Countries BASIC ETL completed: {stats['rows_inserted_sa']} countries")
    return stats


def wrapper_get_countries_geo(**context):
    """
    Wrapper for ETL: Get Countries GEO
    
    Executes loading of geographic fields: API /all (5 fields) → SA geo
    """
    execution_id = context['run_id']
    print(f"🎯 Execution ID: {execution_id}")
    
    stats = etl_get_countries_geo(execution_id=execution_id)
    context['task_instance'].xcom_push(key='countries_geo_stats', value=stats)
    
    print(f"✅ Countries GEO ETL completed: {stats['rows_inserted_sa']} countries")
    return stats


def wrapper_get_countries_culture(**context):
    """
    Wrapper for ETL: Get Countries CULTURE
    
    Executes loading of cultural/political fields: API /all (9 fields) → SA culture
    """
    execution_id = context['run_id']
    print(f"🎯 Execution ID: {execution_id}")
    
    stats = etl_get_countries_culture(execution_id=execution_id)
    context['task_instance'].xcom_push(key='countries_culture_stats', value=stats)
    
    print(f"✅ Countries CULTURE ETL completed: {stats['rows_inserted_sa']} countries")
    return stats


def wrapper_merge_countries_to_th(**context):
    """
    Wrapper for ETL: Merge Countries SA → TH
    
    Combines 3 SA tables (basic + geo + culture) and performs MERGE into TH
    """
    execution_id = context['run_id']
    print(f"🎯 Execution ID: {execution_id}")
    
    stats = etl_merge_countries_to_th(execution_id=execution_id)
    context['task_instance'].xcom_push(key='countries_merged_stats', value=stats)
    
    print(f"✅ MERGE completed: {stats['countries_inserted']} new, {stats['countries_updated']} updated")
    return stats


def wrapper_get_regions_stats(**context):
    """
    Wrapper for ETL: Get Regions Statistics
    
    Executes complete flow: API /region/{region} → Aggregation → SA → TH
    """
    execution_id = context['run_id']
    print(f"🎯 Execution ID: {execution_id}")
    
    stats = etl_get_regions_stats(execution_id=execution_id)
    
    if not stats['success']:
        raise Exception(f"❌ ETL Get Regions Stats failed: {stats['errors']}")
    
    # Save statistics to XCom
    context['task_instance'].xcom_push(key='regions_stats', value=stats)
    
    print(f"✅ ETL completed:")
    print(f"   📥 Regions processed: {', '.join(stats['regions_processed'])}")
    print(f"   📊 Countries extracted: {stats['countries_extracted']}")
    print(f"   💾 SA loaded: {stats['sa_loaded']}")
    print(f"   📥 TH inserted: {stats['th_inserted']}")
    print(f"   🔄 TH updated: {stats['th_updated']}")
    
    return stats


def wrapper_get_weather(**context):
    """
    Wrapper for ETL: Get Weather Data
    
    Executes complete flow: API /forecast → SA → TH (append-only)
    Time series of weather data
    """
    execution_id = context['run_id']
    print(f"🎯 Execution ID: {execution_id}")
    
    stats = etl_get_weather_data(execution_id=execution_id)
    
    if not stats['success']:
        raise Exception(f"❌ ETL Get Weather Data failed: {stats['errors']}")
    
    # Save statistics to XCom
    context['task_instance'].xcom_push(key='weather_stats', value=stats)
    
    print(f"✅ ETL completed:")
    print(f"   📥 Cities processed: {', '.join(stats['countries_processed'])}")
    print(f"   📊 Measurements extracted: {stats['measurements_extracted']}")
    print(f"   💾 SA loaded: {stats['sa_loaded']}")
    print(f"   📥 TH inserted: {stats['th_inserted']}")
    print(f"   🔄 Duplicates ignored: {stats['th_duplicates']}")
    print(f"   📊 Total in TH: {stats['th_total']}")
    
    return stats


def wrapper_get_air_quality(**context):
    """
    Wrapper para ETL: Get Air Quality from AQICN (World Air Quality Index)
def wrapper_get_air_quality(**context):
    """
    Wrapper for ETL: Get Air Quality from AQICN (World Air Quality Index)
    
    Executes complete flow: AQICN API → SA → TH (append-only)
    Time series of air quality data from capitals in th_training_countries
    """
    execution_id = context['run_id']
    print(f"🎯 Execution ID: {execution_id}")
    
    stats = etl_get_air_quality(execution_id=execution_id)
    
    if stats.get('status') != 'SUCCESS':
        raise Exception(f"❌ ETL Get Air Quality failed: {stats.get('status', 'UNKNOWN')}")
    
    # Save statistics to XCom
    context['task_instance'].xcom_push(key='air_quality_stats', value=stats)
    
    print(f"✅ ETL completed:")
    print(f"   📊 Measurements extracted: {stats['measurements_extracted']}")
    print(f"   💾 SA loaded: {stats['rows_in_sa']}")
    print(f"   📥 TH inserted: {stats['th_inserted']}")
    print(f"   🔄 Duplicates ignored: {stats['th_duplicates']}")
    print(f"   📊 Total in TH: {stats['th_total']}")
    
    return stats


# =============================================================================
# DEFINICIÓN DE TAREAS
# =============================================================================

# Tarea 1a: Get Countries BASIC (campos básicos)
task_get_countries_basic = PythonOperator(
    task_id='get_countries_basic',
    python_callable=wrapper_get_countries_basic,
    dag=dag,
    doc_md="""
    ## 🌍 Get Countries BASIC (Paso 1/4)
    
    **Objetivo:** Cargar campos BÁSICOS de todos los países
    
    **Endpoint:** `GET /all?fields=cca2,cca3,name,capital,region,subregion,area,population`
    
    **Flujo:**
    1. EXTRACT: Llamar API (8 campos)
    2. TRANSFORM: Normalizar nombres y estructuras
    3. LOAD SA: TRUNCATE + INSERT en sa_training_countries_basic
    
    **Tabla destino:** `ga_integration.sa_training_countries_basic`
    
    **Por qué separado?**
    - La API REST Countries limita a 10 campos por request
    - Dividimos en 3 tipos de campos: basic, geo, culture
    - Permite paralelizar las 3 extracciones
    - Patrón educativo: múltiples SA → 1 TH
    """,
)

# Tarea 1b: Get Countries GEO (campos geográficos)
task_get_countries_geo = PythonOperator(
    task_id='get_countries_geo',
    python_callable=wrapper_get_countries_geo,
    dag=dag,
    doc_md="""
    ## 🗺️  Get Countries GEO (Paso 2/4)
    
    **Objetivo:** Cargar campos GEOGRÁFICOS de todos los países
    
    **Endpoint:** `GET /all?fields=cca2,cca3,latlng,landlocked,borders`
    
    **Flujo:**
    1. EXTRACT: Llamar API (5 campos)
    2. TRANSFORM: Normalizar coordenadas y fronteras
    3. LOAD SA: TRUNCATE + INSERT en sa_training_countries_geo
    
    **Tabla destino:** `ga_integration.sa_training_countries_geo`
    
    **Datos interesantes:**
    - landlocked: países sin salida al mar (Suiza, Bolivia, etc.)
    - borders: lista de países fronterizos (JSON array)
    - latlng: coordenadas para mapas
    """,
)

# Tarea 1c: Get Countries CULTURE (campos culturales/políticos)
task_get_countries_culture = PythonOperator(
    task_id='get_countries_culture',
    python_callable=wrapper_get_countries_culture,
    dag=dag,
    doc_md="""
    ## 🎭 Get Countries CULTURE (Paso 3/4)
    
    **Objetivo:** Cargar campos CULTURALES/POLÍTICOS de todos los países
    
    **Endpoint:** `GET /all?fields=cca2,cca3,languages,currencies,timezones,flags,independent,unMember,ccn3`
    
    **Flujo:**
    1. EXTRACT: Llamar API (9 campos)
    2. TRANSFORM: Normalizar idiomas, monedas, zonas horarias
    3. LOAD SA: TRUNCATE + INSERT en sa_training_countries_culture
    
    **Tabla destino:** `ga_integration.sa_training_countries_culture`
    
    **Datos interesantes:**
    - languages: idiomas oficiales (JSON object)
    - currencies: monedas (JSON object)
    - timezones: zonas horarias (JSON array)
    - independent: si es país independiente
    - unMember: si es miembro de la ONU
    """,
)

# Tarea 1d: Merge Countries (combinar 3 SA → TH)
task_merge_countries_to_th = PythonOperator(
    task_id='merge_countries_to_th',
    python_callable=wrapper_merge_countries_to_th,
    dag=dag,
    doc_md="""
    ## 🔗 Merge Countries SA → TH (Paso 4/4 - FINAL)
    
    **Objetivo:** Combinar 3 Staging Areas y hacer MERGE en TH
    
    **Fuentes:**
    - sa_training_countries_basic (8 campos)
    - sa_training_countries_geo (5 campos)
    - sa_training_countries_culture (9 campos)
    
    **Flujo:**
    1. READ: JOIN de las 3 SA por code_iso3
    2. MERGE TH: UPSERT en th_training_countries (22 campos totales)
    
    **Tabla destino:** `ga_integration.th_training_countries`
    
    **🎯 CONCEPTO ETL AVANZADO:**
    Este patrón demuestra:
    - Múltiples SA → 1 TH
    - JOIN de staging areas antes del MERGE
    - Enriquecimiento progresivo de datos
    - Paralelización de extracciones + centralización de MERGE
    
    **Ventajas:**
    - API calls en paralelo (3x más rápido)
    - Cumple con límite de 10 campos de la API
    - Todos los campos disponibles en TH
    - Patrón reutilizable para otros casos
    """,
)

# Tarea 2: Get Regions Statistics (ETL completo con agregación)
task_get_regions_stats = PythonOperator(
    task_id='get_regions_stats',
    python_callable=wrapper_get_regions_stats,
    dag=dag,
    doc_md="""
    ## 📊 Get Regions Statistics (ETL Completo + Agregación)
    
    **Objetivo:** Obtener estadísticas agregadas por región geográfica
    
    **Endpoint:** `GET /region/{region}` (múltiples llamadas)
    
    **Regiones:** africa, americas, asia, europe, oceania
    
    **Flujo:**
    1. EXTRACT: Llamar a API /region/{region} para cada región
    2. TRANSFORM: Calcular agregaciones (COUNT, SUM, AVG)
       - Total países por región
       - Población total y promedio
       - Área total
       - Conteo de países landlocked, independientes, miembros ONU
    3. LOAD SA: TRUNCATE + INSERT en sa_training_regions_stats
    4. MERGE TH: UPSERT en th_training_regions_stats
    
    **Tablas afectadas:**
    - `ga_integration.sa_training_regions_stats` (SA - TRUNCATE+INSERT)
    - `ga_integration.th_training_regions_stats` (TH - UPSERT)
    
    **Por qué este enfoque?**
    - Demuestra agregaciones y transformaciones complejas
    - Múltiples llamadas a API con parámetros diferentes
    - Cálculo de métricas derivadas
    """,
)

# Tarea 3: Get Weather Data (ETL completo - Series Temporales)
task_get_weather = PythonOperator(
    task_id='get_weather',
    python_callable=wrapper_get_weather,
    dag=dag,
    doc_md="""
    ## 🌤️ Get Weather Data (ETL Series Temporales)
    
    **Objetivo:** Obtener datos meteorológicos de capitales de países
    
    **Endpoint:** `GET /forecast` (Open-Meteo API)
    
    **Fuente:** https://open-meteo.com/ (API pública gratuita, sin API key)
    
    **Flujo:**
    1. EXTRACT: Llamar a API /forecast para 10 capitales
       - Datos: temperatura, humedad, precipitación, viento
    2. TRANSFORM: Normalizar timestamps y valores
    3. LOAD SA: TRUNCATE + INSERT en sa_training_weather
    4. LOAD TH: INSERT append-only en th_training_weather
    
    **Tablas afectadas:**
    - `ga_integration.sa_training_weather` (SA - TRUNCATE+INSERT)
    - `ga_integration.th_training_weather` (TH - INSERT append-only)
    
    **Diferencias con otras tareas:**
    - ✅ Series temporales (timestamp como partition key)
    - ✅ Estrategia append-only (INSERT, no MERGE)
    - ✅ Constraint UNIQUE para evitar duplicados
    - ✅ Preparada para TimescaleDB hypertables
    - ✅ JOIN con países (country code)
    
    **Por qué append-only?**
    - Cada medición es un punto único en el tiempo
    - No tiene sentido "actualizar" una medición pasada
    - Optimizado para queries de series temporales
    - Compatible con TimescaleDB
    """,
)

# Tarea 4: Get Air Quality (ETL completo - Series Temporales AQICN)
task_get_air_quality = PythonOperator(
    task_id='get_air_quality',
    python_callable=wrapper_get_air_quality,
    dag=dag,
    doc_md=""" ## 🌫️ Get Air Quality Data (ETL Series Temporales - AQICN)
    
    **Objetivo:** Obtener datos de calidad del aire en tiempo real
    
    **Fuente:** World Air Quality Index (AQICN) - https://aqicn.org/api/
    
    **Ciudades monitoreadas:** beijing, shanghai, delhi, london, paris, madrid, new-york, los-angeles, tokyo, seoul
    
    **Flujo:**
    1. EXTRACT: Llamar a AQICN API /feed/{city} para 10 ciudades
       - Datos: AQI, PM2.5, PM10, O3, NO2, SO2, CO, temperatura, humedad
    2. TRANSFORM: Normalizar timestamps y valores
    3. LOAD SA: TRUNCATE + INSERT en sa_training_air_quality
    4. LOAD TH: INSERT append-only en th_training_air_quality
    
    **Tablas afectadas:**
    - `ga_integration.sa_training_air_quality` (SA - TRUNCATE+INSERT)
    - `ga_integration.th_training_air_quality` (TH - INSERT append-only)
    
    **Características:**
    - ✅ API gratuita con 1,000 req/s quota
    - ✅ Token-based authentication (AQICN_API_TOKEN)
    - ✅ Datos en tiempo real de estaciones oficiales
    - ✅ Series temporales con append-only pattern
    - ✅ Constraint UNIQUE (measured_at, station_id)
    - ✅ Índice temporal para queries eficientes
    
    **Métricas monitoreadas:**
    - AQI: Air Quality Index (0-500)
    - PM2.5, PM10: Partículas en suspensión
    - O3, NO2, SO2, CO: Gases contaminantes
    - Temperatura, humedad, presión atmosférica
    
    **Interpretación AQI:**
    - 0-50: Good (Verde)
    - 51-100: Moderate (Amarillo)
    - 101-150: Unhealthy for Sensitive Groups (Naranja)
    - 151-200: Unhealthy (Rojo)
    - 201-300: Very Unhealthy (Púrpura)
    - 301+: Hazardous (Marrón)
    """,
)

# =============================================================================
# DEPENDENCIAS
# =============================================================================

# Flujo completo:
# 1. Extraer 3 tipos de campos en paralelo (basic, geo, culture)
# 2. Combinar las 3 SA en TH (merge)
# 3. Ejecutar regions y weather en paralelo (usan datos de TH countries)

# PASO 1: Extracciones en paralelo (3 API calls simultáneos)
[task_get_countries_basic, task_get_countries_geo, task_get_countries_culture] >> task_merge_countries_to_th

# PASO 2: Después del MERGE, ejecutar aggregaciones y series temporales en paralelo
task_merge_countries_to_th >> [task_get_regions_stats, task_get_weather, task_get_air_quality]

# Esto crea este grafo:
#
#    ┌─────────────────┐
#    │get_countries    │
#    │   _basic        │
#    └────────┬────────┘
#             │
#    ┌────────┼────────┐
#    │get_countries    │        ┌──────────────┐
#    │   _geo          │───────→│merge_        │
#    └────────┬────────┘        │countries_    │
#             │                 │to_th         │
#    ┌────────┼────────┐        └──────┬───────┘
#    │get_countries    │               │
#    │   _culture      │───────────────┘
#    └─────────────────┘
#             
#                               ┌──────┴──────┬──────────────┐
#                               │             │              │
#                        ┌──────▼──────┐ ┌────▼────────┐ ┌──▼─────────┐
#                        │get_regions  │ │get_weather  │ │get_air     │
#                        │   _stats    │ │             │ │  _quality  │
#                        └─────────────┘ └─────────────┘ └────────────┘
#
# Ventajas:
# 1. Paralelización máxima: 3 API calls simultáneos para countries
# 2. Cumple límite de 10 campos de REST Countries API
# 3. Todos los campos disponibles (22 campos totales)
# 4. Patrón educativo: múltiples SA → 1 TH → múltiples análisis
# 5. Optimiza tiempo total de ejecución
# 6. Múltiples fuentes de datos: REST Countries, Open-Meteo, AQICN


# =============================================================================
# DOCUMENTACIÓN DEL DAG
# =============================================================================

dag.doc_md = """
# 🎓 DAG de Formación: Arquitectura ETL con SA-TA-TH

## 🎯 Objetivo Educativo

Este DAG enseña la arquitectura de capas **SA-TA-TH** usando **scripts funcionales**
que agrupan el código por objetivo de negocio (endpoint de la API).

## 🏗️ Arquitectura: Scripts Funcionales

En vez de separar por capa técnica (SA vs TH), organizamos por **objetivo funcional**:

### Enfoque Tradicional (NO usado aquí):
```
scripts/
  ├── extract_sa.py      ← Maneja solo SA
  ├── merge_th.py        ← Maneja solo TH
```

### Enfoque Funcional (USADO aquí):
```
scripts/
  ├── training_get_all_countries.py      ← GET /all (SA + TH completo)
  ├── training_get_regions_stats.py      ← GET /region/{} (SA + TH + agregación)
```

**Ventajas:**
- ✅ Un script = un objetivo completo (cohesión)
- ✅ Fácil de testear independientemente
- ✅ Menor acoplamiento entre componentes
- ✅ Más fácil de mantener y extender

## 📚 Conceptos Clave

### SA - Staging Area (Área de Aterrizaje)
- **Propósito:** Capa temporal para aterrizar datos externos
- **Estrategia:** TRUNCATE + INSERT (refresco completo)
- **Características:**
  - Sin constraints complejos (no PKs, FKs)
  - Optimizada para carga rápida
  - Se puede truncar sin miedo
  - Aislamiento de errores
  
### TA - Tablas Auxiliares
- **Propósito:** Tablas intermedias para:
  - Volumetría alta que no cabe en memoria
  - Cruces complejos entre múltiples fuentes
  - Agregaciones temporales
- **Nota:** No se requiere en ejemplos simples, solo cuando hay mucho volumen o cruces

### TH - Tablas Históricas
- **Propósito:** Persistencia y consulta
- **Estrategia:** MERGE (UPSERT)
- **Características:**
  - Constraints (PKs, FKs, índices)
  - Control de versiones (first_loaded_at, last_updated_at, version)
  - Optimizada para consultas
  - Mantiene histórico de cambios

## 🔄 Flujo del DAG

### Tarea 1: Get All Countries
```
GET /all
   ↓
[Normalize]
   ↓
sa_training_countries (TRUNCATE+INSERT)
   ↓
[MERGE]
   ↓
th_training_countries (UPSERT)
```

### Tarea 2: Get Regions Stats (Paralelo)
```
GET /region/{africa,americas,asia,europe,oceania}
   ↓
[Aggregate: COUNT, SUM, AVG]
   ↓
sa_training_regions_stats (TRUNCATE+INSERT)
   ↓
[MERGE]
   ↓
th_training_regions_stats (UPSERT)
```

## 🌍 Fuente de Datos

**REST Countries API:** https://restcountries.com/

- API pública, sin autenticación
- Datos de todos los países del mundo
- Ideal para formación y demos

## 📊 Tablas Creadas

### Países
- **`sa_training_countries`** - Staging de países (TRUNCATE+INSERT)
- **`th_training_countries`** - Histórico de países (UPSERT con versioning)

### Estadísticas Regionales
- **`sa_training_regions_stats`** - Staging de stats por región (TRUNCATE+INSERT)
- **`th_training_regions_stats`** - Histórico de stats (UPSERT con versioning)

## 🚀 Cómo Usar

### 1. Crear Tablas (una sola vez)
```bash
# Ejecutar DDL inicial
docker exec -i airflow-postgres-1 psql -U goaigua_user -d goaigua_data < scripts/ddl_initial.sql
```

### 2. Ejecutar DAG
- Ir a Airflow UI: http://localhost:8080
- Activar DAG: `training_etl_sa_ta_th`
- Trigger manual: click en "Play" button

### 3. Ver Logs
- Click en la tarea → View Log
- Verás el output completo del ETL

## 📝 Consultas Útiles

```sql
-- Ver todos los países en TH
SELECT code_iso3, name_common, region, population, version
FROM ga_integration.th_training_countries 
ORDER BY population DESC
LIMIT 10;

-- Ver países actualizados recientemente
SELECT code_iso3, name_common, version, last_updated_at
FROM ga_integration.th_training_countries 
WHERE version > 1
ORDER BY last_updated_at DESC;

-- Ver estadísticas por región
SELECT region, total_countries, total_population, avg_population
FROM ga_integration.th_training_regions_stats 
ORDER BY total_population DESC;

-- Países de Europa (usando vista)
SELECT * FROM ga_integration.v_countries_europe LIMIT 10;

-- Top 10 más poblados (usando vista)
SELECT * FROM ga_integration.v_countries_top10_population;

-- Cambios recientes (usando vista)
SELECT * FROM ga_integration.v_countries_recent_changes;
```

## 🧪 Testing Independiente

Puedes ejecutar los scripts directamente para testing:

```bash
# Test: Get All Countries
docker exec -it airflow-scheduler python /opt/airflow/scripts/training_get_all_countries.py

# Test: Get Regions Stats
docker exec -it airflow-scheduler python /opt/airflow/scripts/training_get_regions_stats.py

# Test: Cliente HTTP
docker exec -it airflow-scheduler python /opt/airflow/scripts/training_rest_countries_client.py
```

## 💡 Ejercicios Propuestos

1. **Nuevo script funcional:** Crear `training_get_country_by_code.py` para GET /alpha/{code}
2. **Agregar TA:** Si quisieras cruzar datos de países con otra fuente externa
3. **SCD Type 2:** Modificar TH para mantener histórico completo de cambios (no solo última versión)
4. **Alertas:** Detectar cambios significativos en población entre versiones
5. **Monitoring:** Agregar tarea que compare SA count vs TH count

## 📖 Archivos del Proyecto

```
dags/
  └── training_etl_sa_ta_th.py          ← Este DAG

scripts/
  ├── ddl_initial.sql                   ← Definiciones de tablas SA/TH
  ├── training_rest_countries_client.py ← Cliente HTTP reutilizable
  ├── training_get_all_countries.py     ← Script funcional: GET /all
  └── training_get_regions_stats.py     ← Script funcional: GET /region/{region}
```

## 📖 Referencias

- [Airflow Documentation](https://airflow.apache.org/)
- [REST Countries API](https://restcountries.com/)
- [PostgreSQL UPSERT](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
- [ETL Best Practices](https://en.wikipedia.org/wiki/Extract,_transform,_load)
"""

print(f"✅ DAG 'training_etl_sa_ta_th' cargado correctamente")
