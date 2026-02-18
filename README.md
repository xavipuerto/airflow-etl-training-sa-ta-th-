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

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [REST Countries API](https://restcountries.com/)
- [AQICN API](https://aqicn.org/api/)
- [Data Warehouse Fundamentals](https://en.wikipedia.org/wiki/Data_warehouse)

## 📄 License

MIT License - Feel free to use this for learning and training purposes.

## 👤 Author

Xavier Puerto - [GitHub](https://github.com/xavipuerto)

---

⭐ If you find this training useful, please give it a star!
