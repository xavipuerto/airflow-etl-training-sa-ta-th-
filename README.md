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
├── init-db/
│   └── 01-init-schema.sql      # Database initialization
├── dags/
│   └── training_etl_sa_ta_th.py # Main training DAG
├── scripts/
│   ├── training_rest_countries_client.py
│   ├── training_aqicn_client.py
│   ├── training_weather_client.py
│   ├── training_get_countries_basic.py
│   ├── training_get_countries_geo.py
│   ├── training_get_countries_culture.py
│   ├── training_merge_countries_to_th.py
│   ├── training_get_regions_stats.py
│   ├── training_get_weather.py
│   └── training_get_air_quality_aqicn.py
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
