#!/usr/bin/env python3
"""
ETL: Get Weather Data (Time Series)
==========================================

Functional script that gets weather data from capitals using Open-Meteo API

🎯 Objective: Demonstrate ETL with time series (append-only INSERT)

Flow:
1. EXTRACT: Call Open-Meteo API for country capitals
2. TRANSFORM: Normalize weather data
3. LOAD SA: TRUNCATE + INSERT into sa_training_weather
4. LOAD TH: INSERT (append-only) into th_training_weather

Differences with other ETLs:
- Time series (timestamp + location unique)
- Does NOT use MERGE, only INSERT
- Prepared for TimescaleDB hypertables
"""
import sys
import psycopg2
from datetime import datetime
from typing import Dict, Any, List

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')

from training_weather_client import OpenMeteoClient, extract_weather_data


# =============================================================================
# CONFIGURATION
# =============================================================================

DB_CONFIG = {
    'host': 'postgres-goaigua',
    'port': 5432,
    'database': 'goaigua_data',
    'user': 'goaigua',
    'password': 'goaigua2026',
}

# Capitals of countries to fetch data
# (country_code, city, latitude, longitude)
CAPITALS = [
    ('ES', 'Madrid', 40.4168, -3.7038),
    ('FR', 'Paris', 48.8566, 2.3522),
    ('DE', 'Berlin', 52.5200, 13.4050),
    ('IT', 'Rome', 41.9028, 12.4964),
    ('PT', 'Lisbon', 38.7223, -9.1393),
    ('GB', 'London', 51.5074, -0.1278),
    ('US', 'Washington', 38.9072, -77.0369),
    ('CN', 'Beijing', 39.9042, 116.4074),
    ('JP', 'Tokyo', 35.6762, 139.6503),
    ('BR', 'Brasilia', -15.8267, -47.9218),
]


# =============================================================================
# MAIN FUNCTION: COMPLETE ETL
# =============================================================================

def etl_get_weather_data(execution_id: str = None) -> Dict[str, Any]:
    """
    Complete ETL: GET weather data → SA → TH
    
    Args:
        execution_id: Execution ID (for tracking)
        
    Returns:
        Dict with execution statistics
    """
    if not execution_id:
        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    stats = {
        'success': False,
        'execution_id': execution_id,
        'countries_processed': [],
        'measurements_extracted': 0,
        'sa_loaded': 0,
        'th_inserted': 0,
        'th_duplicates': 0,
        'th_total': 0,
        'errors': [],
    }
    
    print("\n" + "=" * 80)
    print("🌤️  ETL: Get Weather Data")
    print(f"   Endpoint: GET /forecast")
    print(f"   Cities: {len(CAPITALS)}")
    print(f"   Execution ID: {execution_id}")
    print(f"   Timestamp: {datetime.now()}")
    print("=" * 80)
    
    try:
        # =================================================================
        # PHASE 1: EXTRACT - Get data from API
        # =================================================================
        print("\n📥 PHASE 1: EXTRACT - Getting data from Open-Meteo")
        print("-" * 80)
        
        client = OpenMeteoClient()
        measurements = []
        
        for country, city, lat, lon in CAPITALS:
            print(f"\n🌍 Processing: {city}, {country}")
            result = client.get_current_weather(latitude=lat, longitude=lon)
            print(f"   {result}")
            
            if result.ok:
                data = extract_weather_data(result.json_obj, country, city)
                measurements.append(data)
                stats['countries_processed'].append(country)
                print(f"   ✅ Temperature: {data['temperature']}°C, Humidity: {data['humidity']}%")
            else:
                print(f"   ⚠️  Error getting data from {city}")
                stats['errors'].append(f"Error in {city}: {result.status}")
        
        stats['measurements_extracted'] = len(measurements)
        print(f"\n✅ Total extracted: {len(measurements)} measurements from {len(set(stats['countries_processed']))} cities")
        
        if len(measurements) == 0:
            print("⚠️  No measurements extracted. Terminating.")
            return stats
        
        # =================================================================
        # PHASE 2: LOAD SA - Load to Staging Area
        # =================================================================
        print("\n💾 PHASE 2: LOAD SA - Loading to Staging table")
        print("-" * 80)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Truncate SA
        print(f"🗑️  Truncating ga_integration.sa_training_weather...")
        cur.execute("TRUNCATE TABLE ga_integration.sa_training_weather")
        
        # Insert into SA
        print(f"📝 Inserting {len(measurements)} measurements into SA...")
        insert_sa = """
            INSERT INTO ga_integration.sa_training_weather (
                measured_at, country, city, latitude, longitude,
                temperature, humidity, precipitation, wind_speed,
                weather_code, execution_id, loaded_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, NOW()
            )
        """
        
        for m in measurements:
            cur.execute(insert_sa, (
                m['measured_at'], m['country'], m['city'], m['latitude'], m['longitude'],
                m['temperature'], m['humidity'], m['precipitation'], m['wind_speed'],
                m['weather_code'], execution_id
            ))
        
        conn.commit()
        stats['sa_loaded'] = len(measurements)
        print(f"✅ {len(measurements)} measurements loaded into SA")
        
        # =================================================================
        # PHASE 3: LOAD TH - Consolidate SA → TH (Append-only)
        # =================================================================
        print("\n📊 PHASE 3: LOAD TH - Consolidating SA → TH (append-only)")
        print("-" * 80)
        
        # INSERT with ON CONFLICT DO NOTHING (avoid duplicates)
        merge_sql = """
            INSERT INTO ga_integration.th_training_weather (
                measured_at, country, city, latitude, longitude,
                temperature, humidity, precipitation, wind_speed,
                weather_code, execution_id, loaded_at
            )
            SELECT 
                measured_at, country, city, latitude, longitude,
                temperature, humidity, precipitation, wind_speed,
                weather_code, execution_id, loaded_at
            FROM ga_integration.sa_training_weather
            ON CONFLICT (measured_at, city) DO NOTHING
        """
        
        cur.execute(merge_sql)
        stats['th_inserted'] = cur.rowcount
        conn.commit()
        
        # Count duplicates
        stats['th_duplicates'] = len(measurements) - stats['th_inserted']
        
        # Count total in TH
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_weather")
        stats['th_total'] = cur.fetchone()[0]
        
        print(f"✅ INSERT completed:")
        print(f"   📥 Inserted: {stats['th_inserted']}")
        print(f"   🔄 Duplicates ignored: {stats['th_duplicates']}")
        print(f"   📊 Total in TH: {stats['th_total']}")
        
        cur.close()
        conn.close()
        
        stats['success'] = True
        
        # =================================================================
        # SUMMARY
        # =================================================================
        print("\n" + "=" * 80)
        print("📊 SUMMARY ETL: Weather Data")
        print("=" * 80)
        print(f"   Execution ID: {execution_id}")
        print(f"   Status: ✅ SUCCESS")
        print("")
        print(f"   📥 EXTRACT:")
        print(f"      Cities processed: {', '.join(set(stats['countries_processed']))}")
        print(f"      Measurements extracted: {stats['measurements_extracted']}")
        print("")
        print(f"   💾 LOAD SA:")
        print(f"      Truncated: ✅")
        print(f"      Loaded: {stats['sa_loaded']}")
        print("")
        print(f"   📊 LOAD TH:")
        print(f"      Inserted: {stats['th_inserted']}")
        print(f"      Duplicates: {stats['th_duplicates']}")
        print(f"      Total in TH: {stats['th_total']}")
        print("=" * 80)
        
    except Exception as e:
        stats['errors'].append(str(e))
        print(f"\n❌ Error in ETL: {e}")
        import traceback
        traceback.print_exc()
    
    return stats


# =============================================================================
# MAIN: Execute if called directly
# =============================================================================

if __name__ == '__main__':
    print("\n🌤️ " * 40)
    print("Test script: Get Weather Data (Complete ETL)")
    print("🌤️ " * 40)
    
    result = etl_get_weather_data()
    
    if result['success']:
        print("\n✅ ETL executed successfully")
        print("\n💡 Useful queries:")
        print("   -- View data in TH:")
        print(f"   SELECT city, temperature, humidity FROM ga_integration.th_training_weather ORDER BY measured_at DESC LIMIT 10;")
        print("   -- View by country:")
        print(f"   SELECT country, AVG(temperature) as avg_temp FROM ga_integration.th_training_weather GROUP BY country;")
    else:
        print("\n❌ ETL failed. See errors above.")
        sys.exit(1)
