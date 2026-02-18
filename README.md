# airflow-etl-training-sa-ta-th

🎓 Apache Airflow ETL Training: SA-TA-TH Architecture Pattern

Hands-on workshop to learn ETL data warehouse layers:
- **SA (Staging Area)**: Landing zone with TRUNCATE+INSERT
- **TA (Auxiliary Tables)**: Transformations & joins
- **TH (Historical Tables)**: Persistence with MERGE/append-only

Practical exercises using public APIs (REST Countries, AQICN weather & air quality data)

## 🏗️ Architecture Overview

```
┌─────────────────┐
│   Public APIs   │
│  - Countries    │
│  - Weather      │
│  - Air Quality  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   SA (Staging)  │  ← TRUNCATE + INSERT (landing zone)
│   - sa_*        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  TA (Auxiliary) │  ← Transformations & aggregations
│   - ta_*        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ TH (Historical) │  ← MERGE or INSERT append-only
│   - th_*        │
└─────────────────┘
```

### 📚 Theoretical Foundation

This architecture is based on **Ralph Kimball's data warehouse principles** from *"The Data Warehouse ETL Toolkit"*. The SA-TA-TH pattern implements key Kimball concepts:

- **Staging Area (SA)**: Decouples source systems from the warehouse, enabling **idempotent reprocessing** (full refresh with TRUNCATE+INSERT)
- **Transformation Layer (TA)**: Intermediate zone for business logic, ensuring **data lineage and traceability**
- **Historical Layer (TH)**: Implements **Slowly Changing Dimensions (SCD Type 2)** with versioning (`version`, `first_loaded_at`, `last_updated_at`) for **audit trail and time-travel queries**

This layered approach guarantees **process recoverability**, **data quality**, and **end-to-end observability** in ETL pipelines.

## 📁 Project Structure

```
.
├── docker-compose.yaml          # Airflow setup with PostgreSQL
├── .gitignore                   # Git ignore rules
├── init-db/
│   └── 01-init-schema.sql      # Database initialization (SA & TH tables)
├── dags/
│   └── training_etl_sa_ta_th.py # Main training DAG
├── scripts/
│   ├── __init__.py
│   ├── training_rest_countries_client.py    # REST Countries API client
│   ├── training_aqicn_client.py             # AQICN API client  
│   ├── training_weather_client.py           # Weather API client
│   ├── training_get_countries_basic.py      # ETL: Countries basic fields
│   ├── training_get_countries_geo.py        # ETL: Countries geographic data
│   ├── training_get_countries_culture.py    # ETL: Countries cultural data
│   ├── training_merge_countries_to_th.py    # ETL: Merge SA → TH
│   ├── training_get_regions_stats.py        # ETL: Regional statistics
│   ├── training_get_weather.py              # ETL: Weather data
│   └── training_get_air_quality_aqicn.py    # ETL: Air quality data
├── config/
│   └── .gitkeep                # Airflow configuration directory
├── logs/
│   └── .gitkeep                # Airflow logs directory
├── plugins/
│   └── .gitkeep                # Custom Airflow plugins directory
└── README.md
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git

### 1. Clone the repository

```bash
git clone https://github.com/xavipuerto/airflow-etl-training-sa-ta-th-.git
cd airflow-etl-training-sa-ta-th-
```

### 2. Start the environment

```bash
docker-compose up -d
```

This will start:
- Apache Airflow (webserver, scheduler, worker, triggerer)
- PostgreSQL (Airflow metadata)
- PostgreSQL (training data warehouse)
- Redis (Celery backend)

### 3. Access Airflow

- **URL**: http://localhost:8080
- **User**: airflow
- **Password**: airflow

### 4. Run the training DAG

1. Go to the Airflow UI
2. Find the DAG: `training_etl_sa_ta_th`
3. Enable it (toggle on)
4. Trigger manually or wait for the schedule

## 📊 Data Sources

### REST Countries API
- **URL**: https://restcountries.com/
- **Purpose**: Master data (countries, regions, languages)
- **Pattern**: MERGE (updates existing records)

### AQICN/Weather APIs
- **Purpose**: Weather and air quality time series
- **Pattern**: INSERT append-only (time series data)

## 🎓 Learning Objectives

1. **Understand ETL Layers**
   - SA: Temporary landing zone
   - TA: Business transformations
   - TH: Historical persistence

2. **Data Patterns**
   - TRUNCATE + INSERT for staging
   - MERGE for slowly changing dimensions
   - INSERT append-only for time series

3. **Airflow Concepts**
   - DAG definition and scheduling
   - Task dependencies
   - PythonOperator usage
   - Connection management

4. **Real-world APIs**
   - HTTP requests and error handling
   - JSON parsing and transformation
   - Rate limiting and retries

## 🗄️ Database Schema

### Staging Tables (sa_*)
- `sa_countries_basic`
- `sa_countries_geo`
- `sa_countries_culture`
- `sa_regions_stats`
- `sa_weather`
- `sa_air_quality`

### Historical Tables (th_*)
- `th_countries` (MERGE pattern)
- `th_regions_stats` (MERGE pattern)
- `th_weather` (append-only)
- `th_air_quality` (append-only)

## 🔧 Configuration

Database connection is pre-configured in `docker-compose.yaml`:
- **Host**: postgres-goaigua
- **Port**: 5432
- **Database**: goaigua_data
- **User**: goaigua
- **Password**: goaigua2026

## 📝 DAG Workflow

```
get_countries_basic ──┐
                      │
get_countries_geo ────┼──> merge_countries_to_th
                      │
get_countries_culture ┘

get_regions_stats ──> (independent)

get_weather ──> (independent)

get_air_quality ──> (independent)
```

## 🛡️ Best Practices Demonstrated

- ✅ Layered architecture (SA-TA-TH)
- ✅ Idempotent operations
- ✅ Error handling and retries
- ✅ Logging and monitoring
- ✅ Connection pooling
- ✅ SQL injection prevention
- ✅ Transaction management

## 🎯 Hands-on Exercises

### Exercise 1: Understand the SA-TA-TH Flow
1. Run the `training_etl_sa_ta_th` DAG
2. Query the staging tables:
   ```sql
   SELECT * FROM ga_integration.sa_training_countries_basic LIMIT 10;
   SELECT * FROM ga_integration.sa_training_countries_geo LIMIT 10;
   ```
3. Query the historical table:
   ```sql
   SELECT * FROM ga_integration.th_training_countries LIMIT 10;
   ```
4. **Question**: How many countries are there? What's the difference between SA and TH tables?

### Exercise 2: Trace the MERGE Logic
1. Check the MERGE function in `training_merge_countries_to_th.py`
2. Run the DAG twice
3. Query to see what changed:
   ```sql
   SELECT cca3, country_name, updated_at, created_at
   FROM ga_integration.th_training_countries
   WHERE updated_at > created_at;
   ```
4. **Question**: Which countries were updated vs inserted?

### Exercise 3: Time Series Analysis
1. Let the DAG run for several cycles (wait ~30 minutes)
2. Query air quality trends:
   ```sql
   SELECT 
       city,
       DATE_TRUNC('hour', measurement_time) as hour,
       AVG(aqi) as avg_aqi,
       COUNT(*) as measurements
   FROM ga_integration.th_training_air_quality
   GROUP BY city, hour
   ORDER BY hour DESC, city;
   ```
3. **Question**: How does AQI vary across cities? Which city has the best air quality?

### Exercise 4: Modify the DAG
1. Add a new region to process in `training_get_regions_stats.py`
2. Change the schedule interval to 10 minutes
3. **Challenge**: Create a new task to calculate average population by region

### Exercise 5: Error Handling
1. Temporarily break the REST Countries API URL
2. Observe how Airflow handles the failure
3. Fix the URL and trigger the DAG again
4. **Question**: How many retries occurred? Check the task logs

## 📊 Useful SQL Queries

### Check data freshness
```sql
SELECT 
    'SA Basic' as table_name,
    COUNT(*) as row_count,
    MAX(extraction_timestamp) as last_extraction
FROM ga_integration.sa_training_countries_basic

UNION ALL

SELECT 
    'TH Countries',
    COUNT(*),
    MAX(updated_at)
FROM ga_integration.th_training_countries;
```

### Compare SA vs TH
```sql
SELECT 
    sa.cca3,
    sa.country_name as sa_name,
    th.country_name as th_name,
    sa.population as sa_pop,
    th.population as th_pop,
    CASE 
        WHEN sa.population != th.population THEN 'DIFFERENT'
        ELSE 'SAME'
    END as status
FROM ga_integration.sa_training_countries_basic sa
LEFT JOIN ga_integration.th_training_countries th ON sa.cca3 = th.cca3
WHERE sa.population != th.population;
```

### Regional aggregations
```sql
SELECT 
    region,
    COUNT(*) as country_count,
    SUM(population) as total_population,
    AVG(area) as avg_area,
    MAX(population) as largest_country_pop
FROM ga_integration.th_training_countries
GROUP BY region
ORDER BY total_population DESC;
```

## 🐛 Troubleshooting

### Services not starting
```bash
docker-compose down
docker-compose up -d
```

### Check logs
```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker
```

### Reset database
```bash
docker-compose down -v
docker-compose up -d
```

## � TimescaleDB Hypertable Setup (Advanced)

The `th_training_air_quality` table is designed for time-series partitioning with TimescaleDB.

**IMPORTANT**: Execute these commands AFTER running the ETL a few times to populate data.

### 1. Connect to TimescaleDB

```bash
docker exec -it airflow-postgres-goaigua-1 psql -U goaigua -d goaigua_data
```

### 2. Convert to Hypertable (Monthly Partitions)

```sql
-- Create hypertable with monthly partitions
SELECT create_hypertable(
  'ga_integration.th_training_air_quality',
  'measured_at',
  chunk_time_interval => INTERVAL '1 month',
  associated_schema_name => 'ga_integration_part',
  associated_table_prefix => 'th_training_air_quality',
  if_not_exists => true,
  migrate_data => true
);
```

### 3. Configure Compression

```sql
-- Enable compression
ALTER TABLE ga_integration.th_training_air_quality SET (
  timescaledb.compress = true,
  timescaledb.compress_orderby = 'measured_at DESC',
  timescaledb.compress_segmentby = 'station_id'
);

-- Automatic compression policy (compress chunks older than 300 days)
SELECT add_compression_policy(
  'ga_integration.th_training_air_quality',
  INTERVAL '300 days'
);
```

### 4. Insert Test Data (100k rows across 18 months)

```sql
WITH params AS (
  SELECT
    (48 * 30 * 24)::int AS hours_window  -- ~18 months in hours
),
src AS (
  SELECT
    -- Deterministic measured_at: distributes hours backwards within window
    date_trunc('hour', now())
      - ((gs.i % (SELECT hours_window FROM params)) * interval '1 hour') AS measured_at,

    -- Deterministic station_id (dispersed)
    (1000 + ((gs.i * 7919) % 20000))::int AS station_id
  FROM generate_series(1, 100000) AS gs(i)
)

INSERT INTO ga_integration.th_training_air_quality
(
  measured_at, station_id, city_name, country_code,
  latitude, longitude, aqi, dominant_pollutant,
  pm25, pm10, o3, no2, so2, co,
  temperature, humidity, pressure, wind_speed,
  execution_id, loaded_at
)
SELECT
  s.measured_at,
  s.station_id,
  ('City-' || (1 + floor(random() * 800))::int)::varchar(100),
  (ARRAY['ES','IT','FR','DE','GB','AE','SA','PT','NL','BE','TR',
         'PL','GR','RO','SE','FI','IE','CH','DK','NO','US','CA','AU'])[1 + floor(random()*23)]::varchar(2),
  round(((-60 + random()*140)::numeric), 6),
  round(((-170 + random()*340)::numeric), 6),
  (floor(random() * 401))::int,
  (ARRAY['pm25','pm10','o3','no2','so2','co'])[1 + floor(random()*6)]::varchar(10),
  round((random()*250)::numeric, 2),
  round((random()*300)::numeric, 2),
  round((random()*200)::numeric, 2),
  round((random()*200)::numeric, 2),
  round((random()*50 )::numeric, 2),
  round((random()*20 )::numeric, 2),
  round(((-15 + random()*55 )::numeric), 2),
  round(((10 + random()*90 )::numeric), 2),
  round(((950 + random()*100)::numeric), 2),
  round(((0 + random()*20 )::numeric), 2),
  ('scheduled__' || to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS') || '+00:00'),
  now()
FROM src s
ON CONFLICT (station_id, measured_at) DO UPDATE
SET
  city_name = EXCLUDED.city_name,
  country_code = EXCLUDED.country_code,
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  aqi = EXCLUDED.aqi,
  dominant_pollutant = EXCLUDED.dominant_pollutant,
  pm25 = EXCLUDED.pm25,
  pm10 = EXCLUDED.pm10,
  o3 = EXCLUDED.o3,
  no2 = EXCLUDED.no2,
  so2 = EXCLUDED.so2,
  co = EXCLUDED.co,
  temperature = EXCLUDED.temperature,
  humidity = EXCLUDED.humidity,
  pressure = EXCLUDED.pressure,
  wind_speed = EXCLUDED.wind_speed,
  execution_id = EXCLUDED.execution_id,
  loaded_at = EXCLUDED.loaded_at;
```

### 5. Verify Partitions

```sql
-- Check created chunks (monthly partitions)
SELECT * FROM timescaledb_information.chunks
WHERE hypertable_name = 'th_training_air_quality'
ORDER BY chunk_name;

-- Check chunk statistics
SELECT 
  chunk_name,
  range_start,
  range_end,
  num_rows
FROM timescaledb_information.chunks
WHERE hypertable_name = 'th_training_air_quality';
```

### 6. Query Examples with Partition Pruning

```sql
-- Get recent data for specific station (uses partition pruning)
SELECT station_id, city_name, aqi, measured_at
FROM ga_integration.th_training_air_quality
WHERE measured_at >= now() - interval '7 days'
  AND station_id = 5000
ORDER BY measured_at DESC
LIMIT 10;

-- Aggregated air quality by country (last 30 days)
SELECT 
  country_code,
  AVG(aqi) as avg_aqi,
  MAX(aqi) as max_aqi,
  COUNT(*) as measurements
FROM ga_integration.th_training_air_quality
WHERE measured_at >= now() - interval '30 days'
GROUP BY country_code
ORDER BY avg_aqi DESC;
```

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [REST Countries API](https://restcountries.com/)
- [AQICN API](https://aqicn.org/api/)
- [TimescaleDB Documentation](https://docs.timescale.com/)
- [Data Warehouse Fundamentals](https://en.wikipedia.org/wiki/Data_warehouse)

## 📄 License

MIT License - Feel free to use this for learning and training purposes.

## 👤 Author

Xavier Puerto - [GitHub](https://github.com/xavipuerto)

---

⭐ If you find this training useful, please give it a star!
