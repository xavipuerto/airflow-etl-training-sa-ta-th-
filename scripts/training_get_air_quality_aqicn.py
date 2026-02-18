#!/usr/bin/env python3
"""
ETL: Get Air Quality Data (AQICN API)
======================================

**Objetivo Funcional:** Obtener datos de calidad del aire de ciudades principales

**API:** World Air Quality Index (AQICN)
**Endpoint:** GET /feed/{city}

**Flujo ETL:**
1. EXTRACT: Llamar API AQICN para 10 ciudades principales
2. TRANSFORM: Normalizar datos de AQI y contaminantes
3. LOAD SA: TRUNCATE + INSERT en sa_training_air_quality
4. LOAD TH: INSERT append-only en th_training_air_quality (series temporales)

**Tabla destino:** ga_integration.th_training_air_quality

**Concepto ETL:**
- Series temporales: cada medición es un punto único en el tiempo
- Estrategia append-only (INSERT, no MERGE)
- Constraint UNIQUE para evitar duplicados
- Preparado para TimescaleDB hypertables
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import execute_batch

sys.path.insert(0, '/opt/airflow/scripts')
from training_aqicn_client import AQICNClient, extract_air_quality_data


DB_CONFIG = {
    'host': 'postgres-goaigua',
    'port': 5432,
    'database': 'goaigua_data',
    'user': 'goaigua',
    'password': 'goaigua2026',
}


# =============================================================================
# HELPERS
# =============================================================================

def get_cities_from_db() -> List[tuple]:
    """
    Obtiene capitales de países desde th_training_countries
    
    Returns:
        Lista de tuplas (capital, code_iso2) para países que tienen capital definida
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        query = """
        SELECT DISTINCT 
            LOWER(TRIM(jsonb_array_element_text(capital::jsonb, 0))) as capital,
            code_iso2
        FROM ga_integration.th_training_countries
        WHERE capital IS NOT NULL 
          AND capital != ''
          AND capital != '[]'
          AND code_iso2 IS NOT NULL
          AND jsonb_array_length(capital::jsonb) > 0
        ORDER BY capital
        """
        
        cur.execute(query)
        cities = cur.fetchall()
        
        print(f"📊 Se obtuvieron {len(cities)} capitales desde th_training_countries")
        return cities
        
    except Exception as e:
        print(f"❌ Error leyendo ciudades de BD: {e}")
        return []
    finally:
        cur.close()
        conn.close()


# =============================================================================
# EXTRACT
# =============================================================================

def extract_air_quality() -> List[Dict[str, Any]]:
    """
    EXTRACT: Obtiene datos de calidad del aire de capitales de todos los países
    
    Returns:
        Lista de diccionarios con datos de calidad del aire
    """
    print("=" * 80)
    print("📥 EXTRACT: Obteniendo datos de calidad del aire...")
    print("=" * 80)
    
    # Obtener ciudades desde BD
    cities = get_cities_from_db()
    
    if not cities:
        print("⚠️  No se encontraron ciudades en la base de datos")
        return []
    
    client = AQICNClient()
    measurements = []
    cities_processed = 0
    cities_with_data = 0
    
    for city_name, country_code in cities:
        cities_processed += 1
        try:
            # Mostrar progreso cada 50 ciudades
            if cities_processed % 50 == 0:
                print(f"📍 Procesadas {cities_processed}/{len(cities)} ciudades, {cities_with_data} con datos disponibles...")
            
            result = client.get_city_feed(city_name)
            
            if not result.ok:
                continue
            
            data = result.json_obj.get('data', {})
            if not data or data.get('aqi') == '-':
                continue
            
            # Extraer y normalizar
            aq_data = extract_air_quality_data(data)
            aq_data['country_code'] = country_code  # Añadir código de país
            measurements.append(aq_data)
            cities_with_data += 1
            
        except Exception as e:
            # Silenciar errores individuales para no saturar logs
            continue
    
    print(f"\n✅ Se obtuvieron {len(measurements)} mediciones")
    print(f"   📊 Ciudades procesadas: {cities_processed}")
    print(f"   ✅ Con datos disponibles: {cities_with_data}")
    print(f"   ⚠️  Sin datos: {cities_processed - cities_with_data}")
    return measurements


# =============================================================================
# TRANSFORM
# =============================================================================

def transform_measurements(measurements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    TRANSFORM: Normaliza mediciones para base de datos
    
    Args:
        measurements: Lista de mediciones sin procesar
        
    Returns:
        Lista de mediciones transformadas
    """
    print("\n" + "=" * 80)
    print("🔄 TRANSFORM: Normalizando mediciones...")
    print("=" * 80)
    
    transformed = []
    for m in measurements:
        try:
            # Convertir timestamp
            measured_at = m.get('measured_at')
            if not measured_at:
                print(f"   ⚠️  Sin timestamp: {m.get('city_name')}")
                continue
            
            transformed_m = {
                'measured_at': measured_at,
                'station_id': m.get('station_id'),
                'city_name': m.get('city_name'),
                'country_code': m.get('country_code'),
                'latitude': m.get('latitude'),
                'longitude': m.get('longitude'),
                'aqi': m.get('aqi'),
                'dominant_pollutant': m.get('dominant_pollutant'),
                'pm25': m.get('pm25'),
                'pm10': m.get('pm10'),
                'o3': m.get('o3'),
                'no2': m.get('no2'),
                'so2': m.get('so2'),
                'co': m.get('co'),
                'temperature': m.get('temperature'),
                'humidity': m.get('humidity'),
                'pressure': m.get('pressure'),
                'wind_speed': m.get('wind_speed'),
            }
            transformed.append(transformed_m)
            
        except Exception as e:
            print(f"   ⚠️  Error transformando {m.get('city_name')}: {e}")
            continue
    
    print(f"✅ Se transformaron {len(transformed)} mediciones")
    if transformed:
        print(f"   Ejemplo: {transformed[0]['city_name']} - AQI: {transformed[0]['aqi']}, "
              f"PM2.5: {transformed[0]['pm25']}, {transformed[0]['measured_at']}")
    
    return transformed


# =============================================================================
# LOAD SA
# =============================================================================

def load_to_sa(measurements: List[Dict[str, Any]], execution_id: str) -> int:
    """
    LOAD SA: Carga datos en Staging Area (TRUNCATE + INSERT)
    
    Args:
        measurements: Lista de mediciones transformadas
        execution_id: ID único de ejecución
        
    Returns:
        Número de filas insertadas
    """
    print("\n" + "=" * 80)
    print(f"📤 LOAD SA: Cargando en ga_integration.sa_training_air_quality...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # TRUNCATE
        print(f"🗑️  TRUNCATE ga_integration.sa_training_air_quality")
        cur.execute("TRUNCATE TABLE ga_integration.sa_training_air_quality")
        
        # INSERT
        insert_sql = """
        INSERT INTO ga_integration.sa_training_air_quality (
            measured_at, station_id, city_name, country_code,
            latitude, longitude,
            aqi, dominant_pollutant,
            pm25, pm10, o3, no2, so2, co,
            temperature, humidity, pressure, wind_speed,
            execution_id
        ) VALUES (
            %(measured_at)s, %(station_id)s, %(city_name)s, %(country_code)s,
            %(latitude)s, %(longitude)s,
            %(aqi)s, %(dominant_pollutant)s,
            %(pm25)s, %(pm10)s, %(o3)s, %(no2)s, %(so2)s, %(co)s,
            %(temperature)s, %(humidity)s, %(pressure)s, %(wind_speed)s,
            %(execution_id)s
        )
        """
        
        for m in measurements:
            m['execution_id'] = execution_id
        
        print(f"📥 Insertando {len(measurements)} mediciones...")
        execute_batch(cur, insert_sql, measurements, page_size=50)
        
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM ga_integration.sa_training_air_quality")
        count = cur.fetchone()[0]
        
        print(f"✅ Se insertaron {count} mediciones en SA")
        return count
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en LOAD SA: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# =============================================================================
# LOAD TH (INSERT append-only)
# =============================================================================

def load_to_th(execution_id: str) -> Dict[str, int]:
    """
    LOAD TH: Carga desde SA a TH (INSERT append-only para series temporales)
    
    Estrategia:
    - INSERT nuevas mediciones
    - ON CONFLICT DO NOTHING para evitar duplicados
    - No MERGE/UPDATE (las mediciones históricas no se modifican)
    
    Args:
        execution_id: ID de ejecución
        
    Returns:
        Dict con estadísticas (inserted, duplicates, total)
    """
    print("\n" + "=" * 80)
    print(f"📤 LOAD TH: Insertando en ga_integration.th_training_air_quality...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Contar antes
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_air_quality")
        count_before = cur.fetchone()[0]
        
        # INSERT desde SA a TH (append-only, no MERGE)
        insert_sql = """
        INSERT INTO ga_integration.th_training_air_quality (
            measured_at, station_id, city_name, country_code,
            latitude, longitude,
            aqi, dominant_pollutant,
            pm25, pm10, o3, no2, so2, co,
            temperature, humidity, pressure, wind_speed,
            execution_id
        )
        SELECT
            measured_at, station_id, city_name, country_code,
            latitude, longitude,
            aqi, dominant_pollutant,
            pm25, pm10, o3, no2, so2, co,
            temperature, humidity, pressure, wind_speed,
            execution_id
        FROM ga_integration.sa_training_air_quality
        ON CONFLICT (measured_at, station_id)
        DO NOTHING
        """
        
        print(f"📥 Insertando mediciones desde SA (append-only)...")
        cur.execute(insert_sql)
        rows_inserted = cur.rowcount
        
        conn.commit()
        
        # Contar después
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_air_quality")
        count_after = cur.fetchone()[0]
        
        duplicates = len([]) if rows_inserted == 0 else (count_after - count_before - rows_inserted)
        
        print(f"\n✅ LOAD TH completado:")
        print(f"   Mediciones insertadas: {rows_inserted}")
        print(f"   Duplicados ignorados: {duplicates if duplicates > 0 else 0}")
        print(f"   Total en TH: {count_after}")
        
        # Mostrar últimas mediciones
        cur.execute("""
            SELECT city_name, aqi, pm25, dominant_pollutant, measured_at
            FROM ga_integration.th_training_air_quality
            WHERE execution_id = %s
            ORDER BY measured_at DESC
            LIMIT 5
        """, (execution_id,))
        
        print(f"\n📋 Últimas mediciones insertadas:")
        for row in cur.fetchall():
            city, aqi, pm25, pol, ts = row
            print(f"   {city}: AQI {aqi} (PM2.5: {pm25}, {pol}) - {ts}")
        
        return {
            'inserted': rows_inserted,
            'duplicates': max(0, duplicates),
            'total': count_after,
        }
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en LOAD TH: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# =============================================================================
# ORQUESTACIÓN ETL
# =============================================================================

def etl_get_air_quality(execution_id: str = None) -> Dict[str, Any]:
    """
    ETL Completo: Get Air Quality Data
    
    Args:
        execution_id: ID de ejecución (opcional, se genera si no se provee)
        
    Returns:
        Dict con estadísticas de la ejecución
    """
    if execution_id is None:
        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 80)
    print("🌫️  ETL: GET AIR QUALITY DATA (AQICN)")
    print("=" * 80)
    print(f"Execution ID: {execution_id}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Fuente: Capitales desde th_training_countries")
    print("=" * 80)
    
    try:
        # 1. EXTRACT
        measurements_raw = extract_air_quality()
        
        if not measurements_raw:
            print("\n⚠️  No se obtuvieron mediciones")
            return {
                'status': 'NO_DATA',
                'execution_id': execution_id,
                'measurements_extracted': 0,
            }
        
        # 2. TRANSFORM
        measurements_transformed = transform_measurements(measurements_raw)
        
        # 3. LOAD SA
        rows_in_sa = load_to_sa(measurements_transformed, execution_id)
        
        # 4. LOAD TH
        th_stats = load_to_th(execution_id)
        
        # Estadísticas
        stats = {
            'status': 'SUCCESS',
            'execution_id': execution_id,
            'measurements_extracted': len(measurements_raw),
            'measurements_transformed': len(measurements_transformed),
            'rows_in_sa': rows_in_sa,
            'th_inserted': th_stats['inserted'],
            'th_duplicates': th_stats['duplicates'],
            'th_total': th_stats['total'],
        }
        
        print("\n" + "=" * 80)
        print("✅ ETL COMPLETADO EXITOSAMENTE")
        print("=" * 80)
        print(f"Estado: {stats['status']}")
        print(f"Execution ID: {stats['execution_id']}")
        print(f"Mediciones extraídas: {stats['measurements_extracted']}")
        print(f"Mediciones en SA: {stats['rows_in_sa']}")
        print(f"Insertadas en TH: {stats['th_inserted']}")
        print(f"Duplicados ignorados: {stats['th_duplicates']}")
        print(f"Total en TH: {stats['th_total']}")
        print("=" * 80)
        
        return stats
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ETL FALLIDO")
        print("=" * 80)
        print(f"Error: {e}")
        print("=" * 80)
        raise


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    # Ejecutar ETL
    execution_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    stats = etl_get_air_quality(execution_id=execution_id)
    
    print("\n📊 Resumen:")
    print(json.dumps(stats, indent=2))
