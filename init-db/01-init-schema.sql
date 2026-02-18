-- =========================================
-- Script de inicialización de tablas
-- Esquema: ga_integration
-- Base de datos: goaigua_data (PostgreSQL 17)
-- =========================================

-- Crear el esquema ga_integration
CREATE SCHEMA IF NOT EXISTS ga_integration;

-- Dar permisos al usuario goaigua sobre el esquema
GRANT ALL PRIVILEGES ON SCHEMA ga_integration TO goaigua;
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

COMMENT ON SCHEMA ga_integration IS 'Esquema para integración de datos Temetra API';

COMMENT ON TABLE ga_integration.sa_temetra_fdr IS 'Staging: Datos telemétricos FDR de Temetra (puede contener duplicados por ejecución)';
COMMENT ON TABLE ga_integration.th_temetra_fdr IS 'Target/Histórico: Datos FDR consolidados (UPSERT por meter+timestamp)';

COMMENT ON TABLE ga_integration.sa_temetra_meters IS 'Staging: Master data de contadores';
COMMENT ON TABLE ga_integration.th_temetra_meters IS 'Target: Master data de contadores consolidado';
COMMANT ON TABLE ga_integration.th_lpwan_daily_backflow IS 'Target: Datos diarios LPWAN con backflow (volumen retroceso) - CRÍTICO para detección de fugas';
COMMANT ON TABLE ga_integration.th_lpwan_periodic_indices IS 'Target: Índices periódicos LPWAN - complementa datos FDR';
COMMANT ON TABLE ga_integration.th_lpwan_alarms IS 'Target: Alarmas LPWAN específicas de telemetría';
-- =========================================
-- GRANTS (si es necesario)
-- =========================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ga_integration TO goaigua;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ga_integration TO goaigua;

COMMIT;
