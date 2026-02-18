-- =========================================
-- Script de inicialización de tablas
-- Esquema: ga_integration
-- Base de datos: goaigua_data (TimescaleDB on PostgreSQL 17)
-- =========================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Crear el esquema ga_integration
CREATE SCHEMA IF NOT EXISTS ga_integration;

-- Crear el esquema para particiones de TimescaleDB
CREATE SCHEMA IF NOT EXISTS ga_integration_part;

-- Dar permisos al usuario goaigua sobre el esquema
GRANT ALL PRIVILEGES ON SCHEMA ga_integration TO goaigua;
GRANT ALL PRIVILEGES ON SCHEMA ga_integration_part TO goaigua;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ga_integration TO goaigua;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ga_integration TO goaigua;

-- Configurar el search_path por defecto
ALTER DATABASE goaigua_data SET search_path TO ga_integration, public;

-- =========================================
-- TABLAS SA (STAGING AREA)
-- =========================================

-- Routes
CREATE TABLE IF NOT EXISTS ga_integration.sa_temetra_routes (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    route_code VARCHAR(50),
    route_name VARCHAR(255),
    meter_serials JSONB,
    raw_data JSONB
);

-- Meters
CREATE TABLE IF NOT EXISTS ga_integration.sa_temetra_meters (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    meter_serial VARCHAR(50),
    account_ref VARCHAR(50),
    format VARCHAR(20),
    units VARCHAR(20),
    collection_method VARCHAR(50),
    installation_date VARCHAR(20),
    route_code VARCHAR(50),
    raw_data JSONB
);

-- Accounts
CREATE TABLE IF NOT EXISTS ga_integration.sa_temetra_accounts (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    account_ref VARCHAR(50),
    account_name VARCHAR(255),
    address TEXT,
    address_postcode VARCHAR(20),
    phone_number VARCHAR(50),
    customer_email VARCHAR(255),
    raw_data JSONB
);

-- Reads (lecturas manuales)
CREATE TABLE IF NOT EXISTS ga_integration.sa_temetra_reads (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    meter_serial VARCHAR(50),
    timestamp_iso VARCHAR(50),
    timestamp_utc TIMESTAMP,
    index_value BIGINT,
    comment TEXT
);

-- FDR (telemetría - datos de alta frecuencia)
CREATE TABLE IF NOT EXISTS ga_integration.sa_temetra_fdr (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    meter_serial VARCHAR(50),
    timestamp_iso VARCHAR(50),
    timestamp_utc TIMESTAMP,
    index_value BIGINT,
    index_value_str VARCHAR(50),
    date_from VARCHAR(20),
    date_to VARCHAR(20)
);

-- Alarms
CREATE TABLE IF NOT EXISTS ga_integration.sa_temetra_alarms (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    meter_serial VARCHAR(50),
    alarm_date DATE,
    alarm_types JSONB,
    date_from VARCHAR(20),
    date_to VARCHAR(20)
);

-- LPWAN Periodic Indices
CREATE TABLE IF NOT EXISTS ga_integration.sa_lpwan_periodic_indices (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    meter_serial VARCHAR(50),
    timestamp_utc TIMESTAMP,
    index_value BIGINT
);

-- LPWAN Daily Backflow (dato crítico para detección fugas)
CREATE TABLE IF NOT EXISTS ga_integration.sa_lpwan_daily_backflow (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    meter_serial VARCHAR(50),
    date DATE,
    last_index BIGINT,
    backflow_volume DECIMAL(15,3),
    backflow_count INTEGER
);

-- LPWAN Alarms
CREATE TABLE IF NOT EXISTS ga_integration.sa_lpwan_alarms (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    meter_serial VARCHAR(50),
    alarm_date DATE,
    alarm_type VARCHAR(100),
    alarm_description TEXT
);

-- =========================================
-- ÍNDICES SA (opcional - mejora performance)
-- =========================================

CREATE INDEX IF NOT EXISTS idx_sa_temetra_fdr_meter_ts ON ga_integration.sa_temetra_fdr(meter_serial, timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_sa_temetra_fdr_execution ON ga_integration.sa_temetra_fdr(execution_id);
CREATE INDEX IF NOT EXISTS idx_sa_temetra_reads_meter_ts ON ga_integration.sa_temetra_reads(meter_serial, timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_sa_temetra_alarms_meter_date ON ga_integration.sa_temetra_alarms(meter_serial, alarm_date);
CREATE INDEX IF NOT EXISTS idx_sa_lpwan_backflow_meter_date ON ga_integration.sa_lpwan_daily_backflow(meter_serial, date);

-- =========================================
-- TABLAS TH (TARGET/HISTÓRICO)
-- =========================================

-- Routes TH
CREATE TABLE IF NOT EXISTS ga_integration.th_temetra_routes (
    id SERIAL PRIMARY KEY,
    route_code VARCHAR(50) NOT NULL UNIQUE,
    route_name VARCHAR(255),
    meter_serials JSONB,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100)
);

-- Meters TH
CREATE TABLE IF NOT EXISTS ga_integration.th_temetra_meters (
    id SERIAL PRIMARY KEY,
    meter_serial VARCHAR(50) NOT NULL UNIQUE,
    account_ref VARCHAR(50),
    format VARCHAR(20),
    units VARCHAR(20),
    collection_method VARCHAR(50),
    installation_date VARCHAR(20),
    route_code VARCHAR(50),
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100)
);

-- Accounts TH
CREATE TABLE IF NOT EXISTS ga_integration.th_temetra_accounts (
    id SERIAL PRIMARY KEY,
    account_ref VARCHAR(50) NOT NULL UNIQUE,
    account_name VARCHAR(255),
    address TEXT,
    address_postcode VARCHAR(20),
    phone_number VARCHAR(50),
    customer_email VARCHAR(255),
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100)
);

-- Reads TH
CREATE TABLE IF NOT EXISTS ga_integration.th_temetra_reads (
    id SERIAL PRIMARY KEY,
    meter_serial VARCHAR(50) NOT NULL,
    timestamp_utc TIMESTAMP NOT NULL,
    timestamp_iso VARCHAR(50),
    index_value BIGINT,
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100),
    UNIQUE (meter_serial, timestamp_utc)
);

-- FDR TH (tabla más importante - telemetría)
CREATE TABLE IF NOT EXISTS ga_integration.th_temetra_fdr (
    id SERIAL PRIMARY KEY,
    meter_serial VARCHAR(50) NOT NULL,
    timestamp_utc TIMESTAMP NOT NULL,
    timestamp_iso VARCHAR(50),
    index_value BIGINT,
    index_value_str VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100),
    UNIQUE (meter_serial, timestamp_utc)
);

-- Alarms TH
CREATE TABLE IF NOT EXISTS ga_integration.th_temetra_alarms (
    id SERIAL PRIMARY KEY,
    meter_serial VARCHAR(50) NOT NULL,
    alarm_date DATE NOT NULL,
    alarm_types JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100),
    UNIQUE (meter_serial, alarm_date)
);

-- LPWAN Periodic Indices TH
CREATE TABLE IF NOT EXISTS ga_integration.th_lpwan_periodic_indices (
    id SERIAL PRIMARY KEY,
    meter_serial VARCHAR(50) NOT NULL,
    timestamp_utc TIMESTAMP NOT NULL,
    index_value BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100),
    UNIQUE (meter_serial, timestamp_utc)
);

-- LPWAN Daily Backflow TH (CRÍTICO - detecta fugas y anomalías)
CREATE TABLE IF NOT EXISTS ga_integration.th_lpwan_daily_backflow (
    id SERIAL PRIMARY KEY,
    meter_serial VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    last_index BIGINT,
    backflow_volume DECIMAL(15,3),
    backflow_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100),
    UNIQUE (meter_serial, date)
);

-- LPWAN Alarms TH
CREATE TABLE IF NOT EXISTS ga_integration.th_lpwan_alarms (
    id SERIAL PRIMARY KEY,
    meter_serial VARCHAR(50) NOT NULL,
    alarm_date DATE NOT NULL,
    alarm_type VARCHAR(100) NOT NULL,
    alarm_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_execution_id VARCHAR(100),
    UNIQUE (meter_serial, alarm_date, alarm_type)
);

-- =========================================
-- ÍNDICES TH (optimización)
-- =========================================

-- FDR (tabla crítica - muchos índices para performance)
CREATE INDEX IF NOT EXISTS idx_th_temetra_fdr_meter_ts ON ga_integration.th_temetra_fdr(meter_serial, timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_th_temetra_fdr_meter ON ga_integration.th_temetra_fdr(meter_serial);
CREATE INDEX IF NOT EXISTS idx_th_temetra_fdr_ts ON ga_integration.th_temetra_fdr(timestamp_utc DESC);

-- Meters
CREATE INDEX IF NOT EXISTS idx_th_temetra_meters_account ON ga_integration.th_temetra_meters(account_ref);
CREATE INDEX IF NOT EXISTS idx_th_temetra_meters_route ON ga_integration.th_temetra_meters(route_code);

-- LPWAN
CREATE INDEX IF NOT EXISTS idx_th_lpwan_backflow_meter_date ON ga_integration.th_lpwan_daily_backflow(meter_serial, date DESC);
CREATE INDEX IF NOT EXISTS idx_th_lpwan_backflow_date ON ga_integration.th_lpwan_daily_backflow(date DESC);
CREATE INDEX IF NOT EXISTS idx_th_lpwan_backflow_volume ON ga_integration.th_lpwan_daily_backflow(backflow_volume) WHERE backflow_volume > 0;
CREATE INDEX IF NOT EXISTS idx_th_lpwan_alarms_meter ON ga_integration.th_lpwan_alarms(meter_serial, alarm_date DESC);

-- =========================================
-- VISTAS ÚTILES (opcional)
-- =========================================

-- Vista: Consumo diario por meter
CREATE OR REPLACE VIEW ga_integration.v_consumo_diario AS
SELECT 
    meter_serial,
    DATE(timestamp_utc) as fecha,
    COUNT(*) as num_lecturas,
    MIN(index_value) as index_inicio,
    MAX(index_value) as index_fin,
    MAX(index_value) - MIN(index_value) as consumo_dia,
    MIN(timestamp_utc) as primera_lectura,
    MAX(timestamp_utc) as ultima_lectura
FROM ga_integration.th_temetra_fdr
GROUP BY meter_serial, DATE(timestamp_utc);

-- Vista: Últimas lecturas por meter
CREATE OR REPLACE VIEW ga_integration.v_ultima_lectura AS
SELECT DISTINCT ON (meter_serial)
    meter_serial,
    timestamp_utc,
    index_value
FROM ga_integration.th_temetra_fdr
ORDER BY meter_serial, timestamp_utc DESC;

-- Vista: Backflow diario (fugas y anomalías)
CREATE OR REPLACE VIEW ga_integration.v_backflow_resumen AS
SELECT 
    meter_serial,
    date,
    last_index,
    backflow_volume,
    backflow_count,
    CASE 
        WHEN backflow_volume > 100 THEN 'CRÍTICO'
        WHEN backflow_volume > 10 THEN 'ALTO'
        WHEN backflow_volume > 0 THEN 'BAJO'
        ELSE 'NORMAL'
    END as severidad
FROM ga_integration.th_lpwan_daily_backflow
WHERE backflow_volume > 0
ORDER BY date DESC, backflow_volume DESC;

-- Vista: Meters con backflow en últimos 7 días
CREATE OR REPLACE VIEW ga_integration.v_backflow_reciente AS
SELECT 
    meter_serial,
    COUNT(*) as dias_con_backflow,
    SUM(backflow_volume) as backflow_total,
    SUM(backflow_count) as eventos_total,
    MAX(backflow_volume) as backflow_maximo,
    MAX(date) as ultima_fecha
FROM ga_integration.th_lpwan_daily_backflow
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
  AND backflow_volume > 0
GROUP BY meter_serial
ORDER BY backflow_total DESC;

-- =========================================
-- COMENTARIOS
-- =========================================
-- TRAINING TABLES - SA/TA/TH Pattern
-- =========================================

-- SA: Training Countries Basic (campos básicos)
CREATE TABLE IF NOT EXISTS ga_integration.sa_training_countries_basic (
    id SERIAL PRIMARY KEY,
    code_iso2 VARCHAR(2),
    code_iso3 VARCHAR(3),
    name_common VARCHAR(255),
    name_official VARCHAR(255),
    name_native TEXT,
    capital VARCHAR(255),
    region VARCHAR(100),
    subregion VARCHAR(100),
    area NUMERIC(15,2),
    population BIGINT,
    execution_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SA: Training Countries Geo (campos geográficos)
CREATE TABLE IF NOT EXISTS ga_integration.sa_training_countries_geo (
    id SERIAL PRIMARY KEY,
    code_iso2 VARCHAR(2),
    code_iso3 VARCHAR(3),
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    landlocked BOOLEAN,
    borders JSONB,
    execution_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SA: Training Countries Culture (campos culturales/políticos)
CREATE TABLE IF NOT EXISTS ga_integration.sa_training_countries_culture (
    id SERIAL PRIMARY KEY,
    code_iso2 VARCHAR(2),
    code_iso3 VARCHAR(3),
    code_numeric VARCHAR(3),
    languages JSONB,
    currencies JSONB,
    timezones JSONB,
    flag_emoji VARCHAR(10),
    flag_svg TEXT,
    independent BOOLEAN,
    un_member BOOLEAN,
    execution_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TH: Training Countries (tabla histórica consolidada)
CREATE TABLE IF NOT EXISTS ga_integration.th_training_countries (
    id SERIAL PRIMARY KEY,
    code_iso2 VARCHAR(2),
    code_iso3 VARCHAR(3) UNIQUE NOT NULL,
    code_numeric VARCHAR(3),
    name_common VARCHAR(255),
    name_official VARCHAR(255),
    name_native TEXT,
    capital VARCHAR(255),
    region VARCHAR(100),
    subregion VARCHAR(100),
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    area NUMERIC(15,2),
    landlocked BOOLEAN,
    population BIGINT,
    languages JSONB,
    currencies JSONB,
    timezones JSONB,
    borders JSONB,
    flag_emoji VARCHAR(10),
    flag_svg TEXT,
    independent BOOLEAN,
    un_member BOOLEAN,
    first_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version INTEGER DEFAULT 1
);

-- SA: Training Regions Stats (estadísticas por región)
CREATE TABLE IF NOT EXISTS ga_integration.sa_training_regions_stats (
    id SERIAL PRIMARY KEY,
    region VARCHAR(100),
    total_countries INTEGER,
    total_population BIGINT,
    avg_population BIGINT,
    total_area NUMERIC(15,2),
    landlocked_count INTEGER,
    independent_count INTEGER,
    un_member_count INTEGER,
    execution_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TH: Training Regions Stats (histórico de estadísticas)
CREATE TABLE IF NOT EXISTS ga_integration.th_training_regions_stats (
    id SERIAL PRIMARY KEY,
    region VARCHAR(100) UNIQUE NOT NULL,
    total_countries INTEGER,
    total_population BIGINT,
    avg_population BIGINT,
    total_area NUMERIC(15,2),
    landlocked_count INTEGER,
    independent_count INTEGER,
    un_member_count INTEGER,
    first_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version INTEGER DEFAULT 1
);

-- SA: Training Weather (datos meteorológicos)
CREATE TABLE IF NOT EXISTS ga_integration.sa_training_weather (
    id SERIAL PRIMARY KEY,
    measured_at TIMESTAMP,
    country VARCHAR(100),
    city VARCHAR(100),
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    temperature NUMERIC(5,2),
    humidity NUMERIC(5,2),
    precipitation NUMERIC(7,2),
    wind_speed NUMERIC(6,2),
    weather_code INTEGER,
    execution_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TH: Training Weather (histórico de datos meteorológicos - time series)
CREATE TABLE IF NOT EXISTS ga_integration.th_training_weather (
    id SERIAL PRIMARY KEY,
    measured_at TIMESTAMP NOT NULL,
    country VARCHAR(100),
    city VARCHAR(100) NOT NULL,
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    temperature NUMERIC(5,2),
    humidity NUMERIC(5,2),
    precipitation NUMERIC(7,2),
    wind_speed NUMERIC(6,2),
    weather_code INTEGER,
    execution_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (measured_at, city)
);

-- SA: Training Air Quality (calidad del aire)
CREATE TABLE IF NOT EXISTS ga_integration.sa_training_air_quality (
    id SERIAL PRIMARY KEY,
    measured_at TIMESTAMP,
    station_id INTEGER,
    city_name VARCHAR(100),
    country_code VARCHAR(2),
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    aqi INTEGER,
    dominant_pollutant VARCHAR(10),
    pm25 NUMERIC(10,2),
    pm10 NUMERIC(10,2),
    o3 NUMERIC(10,2),
    no2 NUMERIC(10,2),
    so2 NUMERIC(10,2),
    co NUMERIC(10,2),
    temperature NUMERIC(5,2),
    humidity NUMERIC(5,2),
    pressure NUMERIC(7,2),
    wind_speed NUMERIC(6,2),
    execution_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TH: Training Air Quality (histórico de calidad del aire - time series)
CREATE TABLE IF NOT EXISTS ga_integration.th_training_air_quality (
    id SERIAL PRIMARY KEY,
    measured_at TIMESTAMP NOT NULL,
    station_id INTEGER NOT NULL,
    city_name VARCHAR(100),
    country_code VARCHAR(2),
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    aqi INTEGER,
    dominant_pollutant VARCHAR(10),
    pm25 NUMERIC(10,2),
    pm10 NUMERIC(10,2),
    o3 NUMERIC(10,2),
    no2 NUMERIC(10,2),
    so2 NUMERIC(10,2),
    co NUMERIC(10,2),
    temperature NUMERIC(5,2),
    humidity NUMERIC(5,2),
    pressure NUMERIC(7,2),
    wind_speed NUMERIC(6,2),
    execution_id VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (measured_at, station_id)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_th_countries_code_iso3 ON ga_integration.th_training_countries(code_iso3);
CREATE INDEX IF NOT EXISTS idx_th_countries_region ON ga_integration.th_training_countries(region);
CREATE INDEX IF NOT EXISTS idx_th_weather_measured_at ON ga_integration.th_training_weather(measured_at);
CREATE INDEX IF NOT EXISTS idx_th_weather_city ON ga_integration.th_training_weather(city);
CREATE INDEX IF NOT EXISTS idx_th_air_quality_measured_at ON ga_integration.th_training_air_quality(measured_at);
CREATE INDEX IF NOT EXISTS idx_th_air_quality_station ON ga_integration.th_training_air_quality(station_id);

-- =========================================

COMMENT ON SCHEMA ga_integration IS 'Esquema para integración de datos Temetra API';

COMMENT ON TABLE ga_integration.sa_temetra_fdr IS 'Staging: Datos telemétricos FDR de Temetra (puede contener duplicados por ejecución)';
COMMENT ON TABLE ga_integration.th_temetra_fdr IS 'Target/Histórico: Datos FDR consolidados (UPSERT por meter+timestamp)';

COMMENT ON TABLE ga_integration.sa_temetra_meters IS 'Staging: Master data de contadores';
COMMENT ON TABLE ga_integration.th_temetra_meters IS 'Target: Master data de contadores consolidado';
COMMENT ON TABLE ga_integration.th_lpwan_daily_backflow IS 'Target: Datos diarios LPWAN con backflow (volumen retroceso) - CRÍTICO para detección de fugas';
COMMENT ON TABLE ga_integration.th_lpwan_periodic_indices IS 'Target: Índices periódicos LPWAN - complementa datos FDR';
COMMENT ON TABLE ga_integration.th_lpwan_alarms IS 'Target: Alarmas LPWAN específicas de telemetría';

COMMENT ON TABLE ga_integration.sa_training_countries_basic IS 'Staging: Países - campos básicos';
COMMENT ON TABLE ga_integration.sa_training_countries_geo IS 'Staging: Países - campos geográficos';
COMMENT ON TABLE ga_integration.sa_training_countries_culture IS 'Staging: Países - campos culturales/políticos';
COMMENT ON TABLE ga_integration.th_training_countries IS 'Target: Países consolidados con versionado';
COMMENT ON TABLE ga_integration.sa_training_regions_stats IS 'Staging: Estadísticas por región';
COMMENT ON TABLE ga_integration.th_training_regions_stats IS 'Target: Estadísticas por región consolidadas';
COMMENT ON TABLE ga_integration.sa_training_weather IS 'Staging: Datos meteorológicos';
COMMENT ON TABLE ga_integration.th_training_weather IS 'Target: Histórico de datos meteorológicos (time series)';
COMMENT ON TABLE ga_integration.sa_training_air_quality IS 'Staging: Calidad del aire';
COMMENT ON TABLE ga_integration.th_training_air_quality IS 'Target: Histórico de calidad del aire (time series)';

-- =========================================
-- GRANTS (si es necesario)
-- =========================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ga_integration TO goaigua;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ga_integration TO goaigua;

-- =========================================
-- TIMESCALEDB HYPERTABLE SETUP (Manual Steps)
-- =========================================

/*
 IMPORTANT: Execute these commands AFTER running the ETL a few times to populate data.

 (1) Connect to the database:
     psql -h localhost -p 5433 -U goaigua -d goaigua_data

 (2) Convert th_training_air_quality to hypertable with monthly partitions:

     SELECT create_hypertable(
       'ga_integration.th_training_air_quality',
       'measured_at',
       chunk_time_interval => INTERVAL '1 month',
       associated_schema_name => 'ga_integration_part',
       associated_table_prefix => 'th_training_air_quality',
       if_not_exists => true,
       migrate_data => true
     );

 (3) Create additional recommended indexes:

     CREATE INDEX IF NOT EXISTS idx_th_air_quality_measured_at
       ON ga_integration.th_training_air_quality (measured_at);

     CREATE INDEX IF NOT EXISTS idx_th_air_quality_station
       ON ga_integration.th_training_air_quality (station_id);

     CREATE INDEX IF NOT EXISTS idx_th_air_quality_station_measured
       ON ga_integration.th_training_air_quality (station_id, measured_at DESC);

 (4) Configure compression:

     ALTER TABLE ga_integration.th_training_air_quality SET (
       timescaledb.compress = true,
       timescaledb.compress_orderby = 'measured_at DESC',
       timescaledb.compress_segmentby = 'station_id'
     );

 (5) Add automatic compression policy (compress chunks > 300 days):

     SELECT add_compression_policy(
       'ga_integration.th_training_air_quality',
       INTERVAL '300 days'
     );

 (6) Insert 5000 test rows distributed over the last 30 days with UPSERT:

     INSERT INTO ga_integration.th_training_air_quality
     (
       measured_at, station_id, city_name, country_code,
       latitude, longitude, aqi, dominant_pollutant,
       pm25, pm10, o3, no2, so2, co,
       temperature, humidity, pressure, wind_speed,
       execution_id, loaded_at
     )
     SELECT
       date_trunc('hour', now() - (random() * interval '30 days')) AS measured_at,
       (1000 + floor(random() * 20000))::int AS station_id,
       ('City-' || (1 + floor(random() * 500))::int)::varchar(100) AS city_name,
       (ARRAY['ES','IT','FR','DE','GB','AE','SA','PT','NL','BE','TR','PL','GR','RO','SE','FI','IE','CH','DK','NO'])[1 + floor(random()*20)]::varchar(2) AS country_code,
       round(((-60 + random()*140)::numeric), 6) AS latitude,
       round(((-170 + random()*340)::numeric), 6) AS longitude,
       (floor(random() * 401))::int AS aqi,
       CASE
         WHEN random() < 0.05 THEN NULL
         ELSE (ARRAY['pm25','pm10','o3','no2','so2','co'])[1 + floor(random()*6)]::varchar(10)
       END AS dominant_pollutant,
       CASE WHEN random() < 0.10 THEN NULL ELSE round((random()*250)::numeric, 2) END AS pm25,
       CASE WHEN random() < 0.15 THEN NULL ELSE round((random()*300)::numeric, 2) END AS pm10,
       CASE WHEN random() < 0.20 THEN NULL ELSE round((random()*200)::numeric, 2) END AS o3,
       CASE WHEN random() < 0.20 THEN NULL ELSE round((random()*200)::numeric, 2) END AS no2,
       CASE WHEN random() < 0.25 THEN NULL ELSE round((random()*50 )::numeric, 2) END AS so2,
       CASE WHEN random() < 0.30 THEN NULL ELSE round((random()*20 )::numeric, 2) END AS co,
       round(((-10 + random()*50 )::numeric), 2) AS temperature,
       round(((10 + random()*90 )::numeric), 2) AS humidity,
       round(((950 + random()*100)::numeric), 2) AS pressure,
       round(((0 + random()*15 )::numeric), 2) AS wind_speed,
       ('scheduled__' || to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS.MS') || '+00:00')::varchar(100) AS execution_id,
       now() AS loaded_at
     FROM generate_series(1, 5000)
     ON CONFLICT (measured_at, station_id) DO UPDATE
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

 (7) Verify partitions created:

     SELECT * FROM timescaledb_information.chunks
     WHERE hypertable_name = 'th_training_air_quality'
     ORDER BY chunk_name;

 (8) Query example with partition pruning:

     SELECT station_id, city_name, aqi, measured_at
     FROM ga_integration.th_training_air_quality
     WHERE measured_at >= now() - interval '7 days'
       AND station_id = 5000
     ORDER BY measured_at DESC
     LIMIT 10;
*/

COMMIT;
