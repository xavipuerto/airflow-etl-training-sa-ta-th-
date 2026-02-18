#!/usr/bin/env python3
"""
ETL: Get Weather Data (Series Temporales)
==========================================

Script funcional que obtiene datos meteorológicos de capitales usando Open-Meteo API

🎯 Objetivo: Demostrar ETL con series temporales (append-only INSERT)

Flujo:
1. EXTRACT: Llamar a Open-Meteo API para capitales de países
2. TRANSFORM: Normalizar datos de clima
3. LOAD SA: TRUNCATE + INSERT en sa_training_weather
4. LOAD TH: INSERT (append-only) en th_training_weather

Diferencias con otros ETLs:
- Series temporales (timestamp + location unique)
- NO usa MERGE, solo INSERT
- Preparado para TimescaleDB hypertables
"""
import sys
import psycopg2
from datetime import datetime
from typing import Dict, Any, List

# Agregar scripts al path
sys.path.insert(0, '/opt/airflow/scripts')

from training_weather_client import OpenMeteoClient, extract_weather_data


# =============================================================================
# CONFIGURACIÓN
# =============================================================================

DB_CONFIG = {
    'host': 'postgres-goaigua',
    'port': 5432,
    'database': 'goaigua_data',
    'user': 'goaigua',
    'password': 'goaigua2026',
}

# Capitales de países para obtener datos
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
# FUNCIÓN PRINCIPAL: ETL COMPLETO
# =============================================================================

def etl_get_weather_data(execution_id: str = None) -> Dict[str, Any]:
    """
    ETL completo: GET weather data → SA → TH
    
    Args:
        execution_id: ID de la ejecución (para tracking)
        
    Returns:
        Dict con estadísticas de la ejecución
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
    print("🌤️  ETL: Obtener Datos Meteorológicos")
    print(f"   Endpoint: GET /forecast")
    print(f"   Ciudades: {len(CAPITALS)}")
    print(f"   Execution ID: {execution_id}")
    print(f"   Timestamp: {datetime.now()}")
    print("=" * 80)
    
    try:
        # =================================================================
        # FASE 1: EXTRACT - Obtener datos de la API
        # =================================================================
        print("\n📥 FASE 1: EXTRACT - Obteniendo datos de Open-Meteo")
        print("-" * 80)
        
        client = OpenMeteoClient()
        measurements = []
        
        for country, city, lat, lon in CAPITALS:
            print(f"\n🌍 Procesando: {city}, {country}")
            result = client.get_current_weather(latitude=lat, longitude=lon)
            print(f"   {result}")
            
            if result.ok:
                data = extract_weather_data(result.json_obj, country, city)
                measurements.append(data)
                stats['countries_processed'].append(country)
                print(f"   ✅ Temperatura: {data['temperature']}°C, Humedad: {data['humidity']}%")
            else:
                print(f"   ⚠️  Error al obtener datos de {city}")
                stats['errors'].append(f"Error en {city}: {result.status}")
        
        stats['measurements_extracted'] = len(measurements)
        print(f"\n✅ Total extraído: {len(measurements)} mediciones de {len(set(stats['countries_processed']))} ciudades")
        
        if len(measurements) == 0:
            print("⚠️  No se extrajeron mediciones. Terminando.")
            return stats
        
        # =================================================================
        # FASE 2: LOAD SA - Cargar a Staging Area
        # =================================================================
        print("\n💾 FASE 2: LOAD SA - Cargando a tabla Staging")
        print("-" * 80)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Truncar SA
        print(f"🗑️  Truncando ga_integration.sa_training_weather...")
        cur.execute("TRUNCATE TABLE ga_integration.sa_training_weather")
        
        # Insertar en SA
        print(f"📝 Insertando {len(measurements)} mediciones en SA...")
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
        print(f"✅ {len(measurements)} mediciones cargadas en SA")
        
        # =================================================================
        # FASE 3: LOAD TH - Consolidar SA → TH (Append-only)
        # =================================================================
        print("\n📊 FASE 3: LOAD TH - Consolidando SA → TH (append-only)")
        print("-" * 80)
        
        # INSERT con ON CONFLICT DO NOTHING (evitar duplicados)
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
        
        # Contar duplicados
        stats['th_duplicates'] = len(measurements) - stats['th_inserted']
        
        # Contar total en TH
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_weather")
        stats['th_total'] = cur.fetchone()[0]
        
        print(f"✅ INSERT completado:")
        print(f"   📥 Insertadas: {stats['th_inserted']}")
        print(f"   🔄 Duplicados ignorados: {stats['th_duplicates']}")
        print(f"   📊 Total en TH: {stats['th_total']}")
        
        cur.close()
        conn.close()
        
        stats['success'] = True
        
        # =================================================================
        # RESUMEN
        # =================================================================
        print("\n" + "=" * 80)
        print("📊 RESUMEN ETL: Weather Data")
        print("=" * 80)
        print(f"   Execution ID: {execution_id}")
        print(f"   Estado: ✅ EXITOSO")
        print("")
        print(f"   📥 EXTRACT:")
        print(f"      Ciudades procesadas: {', '.join(set(stats['countries_processed']))}")
        print(f"      Mediciones extraídas: {stats['measurements_extracted']}")
        print("")
        print(f"   💾 LOAD SA:")
        print(f"      Truncado: ✅")
        print(f"      Cargadas: {stats['sa_loaded']}")
        print("")
        print(f"   📊 LOAD TH:")
        print(f"      Insertadas: {stats['th_inserted']}")
        print(f"      Duplicados: {stats['th_duplicates']}")
        print(f"      Total en TH: {stats['th_total']}")
        print("=" * 80)
        
    except Exception as e:
        stats['errors'].append(str(e))
        print(f"\n❌ Error en ETL: {e}")
        import traceback
        traceback.print_exc()
    
    return stats


# =============================================================================
# MAIN: Ejecutar si se llama directamente
# =============================================================================

if __name__ == '__main__':
    print("\n🌤️ " * 40)
    print("Script de prueba: Get Weather Data (ETL completo)")
    print("🌤️ " * 40)
    
    result = etl_get_weather_data()
    
    if result['success']:
        print("\n✅ ETL ejecutado exitosamente")
        print("\n💡 Consultas útiles:")
        print("   -- Ver datos en TH:")
        print(f"   SELECT city, temperature, humidity FROM ga_integration.th_training_weather ORDER BY measured_at DESC LIMIT 10;")
        print("   -- Ver por país:")
        print(f"   SELECT country, AVG(temperature) as avg_temp FROM ga_integration.th_training_weather GROUP BY country;")
    else:
        print("\n❌ ETL falló. Ver errores arriba.")
        sys.exit(1)
