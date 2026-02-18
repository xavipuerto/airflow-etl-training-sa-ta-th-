#!/usr/bin/env python3
"""
ETL: Get Air Quality Data (AQICN API)
======================================

**Functional Objective:** Get air quality data from major cities

**API:** World Air Quality Index (AQICN)
**Endpoint:** GET /feed/{city}

**ETL Flow:**
1. EXTRACT: Call AQICN API for 10 major cities
2. TRANSFORM: Normalize AQI and pollutant data
3. LOAD SA: TRUNCATE + INSERT into sa_training_air_quality
4. LOAD TH: INSERT append-only into th_training_air_quality (time series)

**Target table:** ga_integration.th_training_air_quality

**ETL Concept:**
- Time series: each measurement is a unique point in time
- Append-only strategy (INSERT, not MERGE)
- UNIQUE constraint to avoid duplicates
- Prepared for TimescaleDB hypertables
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
    Get country capitals from th_training_countries
    
    Returns:
        List of tuples (capital, code_iso2) for countries that have a defined capital
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
        
        print(f"📊 Retrieved {len(cities)} capitals from th_training_countries")
        return cities
        
    except Exception as e:
        print(f"❌ Error reading cities from DB: {e}")
        return []
    finally:
        cur.close()
        conn.close()


# =============================================================================
# EXTRACT
# =============================================================================

def extract_air_quality() -> List[Dict[str, Any]]:
    """
    EXTRACT: Get air quality data from capitals of all countries
    
    Returns:
        List of dictionaries with air quality data
    """
    print("=" * 80)
    print("📥 EXTRACT: Getting air quality data...")
    print("=" * 80)
    
    # Get cities from DB
    cities = get_cities_from_db()
    
    if not cities:
        print("⚠️  No cities found in database")
        return []
    
    client = AQICNClient()
    measurements = []
    cities_processed = 0
    cities_with_data = 0
    
    for city_name, country_code in cities:
        cities_processed += 1
        try:
            # Show progress every 50 cities
            if cities_processed % 50 == 0:
                print(f"📍 Processed {cities_processed}/{len(cities)} cities, {cities_with_data} with available data...")
            
            result = client.get_city_feed(city_name)
            
            if not result.ok:
                continue
            
            data = result.json_obj.get('data', {})
            if not data or data.get('aqi') == '-':
                continue
            
            # Extract and normalize
            aq_data = extract_air_quality_data(data)
            aq_data['country_code'] = country_code  # Add country code
            measurements.append(aq_data)
            cities_with_data += 1
            
        except Exception as e:
            # Silence individual errors to avoid saturating logs
            continue
    
    print(f"\n✅ Retrieved {len(measurements)} measurements")
    print(f"   📊 Cities processed: {cities_processed}")
    print(f"   ✅ With available data: {cities_with_data}")
    print(f"   ⚠️  Without data: {cities_processed - cities_with_data}")
    return measurements


# =============================================================================
# TRANSFORM
# =============================================================================

def transform_measurements(measurements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    TRANSFORM: Normalize measurements for database
    
    Args:
        measurements: List of unprocessed measurements
        
    Returns:
        List of transformed measurements
    """
    print("\n" + "=" * 80)
    print("🔄 TRANSFORM: Normalizing measurements...")
    print("=" * 80)
    
    transformed = []
    for m in measurements:
        try:
            # Convert timestamp
            measured_at = m.get('measured_at')
            if not measured_at:
                print(f"   ⚠️  No timestamp: {m.get('city_name')}")
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
            print(f"   ⚠️  Error transforming {m.get('city_name')}: {e}")
            continue
    
    print(f"✅ Transformed {len(transformed)} measurements")
    if transformed:
        print(f"   Example: {transformed[0]['city_name']} - AQI: {transformed[0]['aqi']}, "
              f"PM2.5: {transformed[0]['pm25']}, {transformed[0]['measured_at']}")
    
    return transformed


# =============================================================================
# LOAD SA
# =============================================================================

def load_to_sa(measurements: List[Dict[str, Any]], execution_id: str) -> int:
    """
    LOAD SA: Load data into Staging Area (TRUNCATE + INSERT)
    
    Args:
        measurements: List of transformed measurements
        execution_id: Unique execution ID
        
    Returns:
        Number of rows inserted
    """
    print("\n" + "=" * 80)
    print(f"📤 LOAD SA: Loading into ga_integration.sa_training_air_quality...")
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
        
        print(f"📥 Inserting {len(measurements)} measurements...")
        execute_batch(cur, insert_sql, measurements, page_size=50)
        
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM ga_integration.sa_training_air_quality")
        count = cur.fetchone()[0]
        
        print(f"✅ Inserted {count} measurements into SA")
        return count
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error in LOAD SA: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# =============================================================================
# LOAD TH (INSERT append-only)
# =============================================================================

def load_to_th(execution_id: str) -> Dict[str, int]:
    """
    LOAD TH: Load from SA to TH (INSERT append-only for time series)
    
    Strategy:
    - INSERT new measurements
    - ON CONFLICT DO NOTHING to avoid duplicates
    - No MERGE/UPDATE (historical measurements are not modified)
    
    Args:
        execution_id: Execution ID
        
    Returns:
        Dict with statistics (inserted, duplicates, total)
    """
    print("\n" + "=" * 80)
    print(f"📤 LOAD TH: Inserting into ga_integration.th_training_air_quality...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Count before
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_air_quality")
        count_before = cur.fetchone()[0]
        
        # INSERT from SA to TH (append-only, not MERGE)
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
        
        print(f"📥 Inserting measurements from SA (append-only)...")
        cur.execute(insert_sql)
        rows_inserted = cur.rowcount
        
        conn.commit()
        
        # Count after
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_air_quality")
        count_after = cur.fetchone()[0]
        
        duplicates = len([]) if rows_inserted == 0 else (count_after - count_before - rows_inserted)
        
        print(f"\n✅ LOAD TH completed:")
        print(f"   Measurements inserted: {rows_inserted}")
        print(f"   Duplicates ignored: {duplicates if duplicates > 0 else 0}")
        print(f"   Total in TH: {count_after}")
        
        # Show latest measurements
        cur.execute("""
            SELECT city_name, aqi, pm25, dominant_pollutant, measured_at
            FROM ga_integration.th_training_air_quality
            WHERE execution_id = %s
            ORDER BY measured_at DESC
            LIMIT 5
        """, (execution_id,))
        
        print(f"\n📋 Latest inserted measurements:")
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
        print(f"❌ Error in LOAD TH: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# =============================================================================
# ETL ORCHESTRATION
# =============================================================================

def etl_get_air_quality(execution_id: str = None) -> Dict[str, Any]:
    """
    Complete ETL: Get Air Quality Data
    
    Args:
        execution_id: Execution ID (optional, generated if not provided)
        
    Returns:
        Dict with execution statistics
    """
    if execution_id is None:
        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 80)
    print("🌫️  ETL: GET AIR QUALITY DATA (AQICN)")
    print("=" * 80)
    print(f"Execution ID: {execution_id}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Source: Capitals from th_training_countries")
    print("=" * 80)
    
    try:
        # 1. EXTRACT
        measurements_raw = extract_air_quality()
        
        if not measurements_raw:
            print("\n⚠️  No measurements obtained")
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
        
        # Statistics
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
        print("✅ ETL COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"Status: {stats['status']}")
        print(f"Execution ID: {stats['execution_id']}")
        print(f"Measurements extracted: {stats['measurements_extracted']}")
        print(f"Measurements in SA: {stats['rows_in_sa']}")
        print(f"Inserted in TH: {stats['th_inserted']}")
        print(f"Duplicates ignored: {stats['th_duplicates']}")
        print(f"Total in TH: {stats['th_total']}")
        print("=" * 80)
        
        return stats
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ETL FAILED")
        print("=" * 80)
        print(f"Error: {e}")
        print("=" * 80)
        raise


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    # Execute ETL
    execution_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    stats = etl_get_air_quality(execution_id=execution_id)
    
    print("\n📊 Summary:")
    print(json.dumps(stats, indent=2))
