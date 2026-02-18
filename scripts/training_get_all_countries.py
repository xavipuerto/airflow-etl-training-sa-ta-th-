#!/usr/bin/env python3
"""
Script Funcional: Obtener TODOS los países
===========================================

Endpoint: GET /all
Objetivo: Extraer todos los países del mundo

Flujo completo SA → TH:
1. EXTRACT: Llamar API /all
2. LOAD SA: TRUNCATE + INSERT en sa_training_countries
3. MERGE TH: UPSERT de SA a th_training_countries

Este script demuestra un flujo ETL completo end-to-end
"""
import sys
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Any
from datetime import datetime

sys.path.insert(0, '/opt/airflow/scripts')
from training_rest_countries_client import RestCountriesClient, extract_country_data


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

SCHEMA = 'ga_integration'
TABLE_SA = 'sa_training_countries'
TABLE_TH = 'th_training_countries'


# =============================================================================
# FUNCIÓN PRINCIPAL: ETL COMPLETO
# =============================================================================

def etl_get_all_countries(execution_id: str = None) -> Dict[str, Any]:
    """
    ETL completo: GET /all → SA → TH
    
    Args:
        execution_id: ID de la ejecución
        
    Returns:
        Dict con estadísticas completas
    """
    if not execution_id:
        execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    start_time = datetime.now()
    print(f"\n{'='*80}")
    print(f"🌍 ETL: Obtener TODOS los países")
    print(f"   Endpoint: GET /all")
    print(f"   Execution ID: {execution_id}")
    print(f"   Timestamp: {start_time}")
    print(f"{'='*80}\n")
    
    stats = {
        'execution_id': execution_id,
        'start_time': start_time,
        'endpoint': '/all',
        
        # Fase EXTRACT
        'api_calls': 0,
        'countries_extracted': 0,
        
        # Fase LOAD SA
        'sa_truncated': False,
        'sa_loaded': 0,
        
        # Fase MERGE TH
        'th_inserted': 0,
        'th_updated': 0,
        'th_total': 0,
        
        'errors': [],
        'success': False,
    }
    
    conn = None
    
    try:
        # =====================================================================
        # FASE 1: EXTRACT - Llamar a la API
        # =====================================================================
        print("📥 FASE 1: EXTRACT - Obteniendo datos de la API")
        print("-" * 80)
        
        client = RestCountriesClient()
        result = client.get_all_countries()
        stats['api_calls'] += 1
        
        print(result)
        
        if not result.ok:
            error_msg = f"Error en API: {result.status} - {result.text[:200]}"
            stats['errors'].append(error_msg)
            print(f"❌ {error_msg}")
            return stats
        
        countries = result.json_obj
        stats['countries_extracted'] = len(countries)
        print(f"✅ Extraídos {len(countries)} países de la API")
        
        # Normalizar datos
        print(f"\n📊 Normalizando datos...")
        normalized_countries = []
        for country in countries:
            try:
                data = extract_country_data(country)
                data['execution_id'] = execution_id
                data['loaded_at'] = datetime.now()
                normalized_countries.append(data)
            except Exception as e:
                code = country.get('cca3', 'UNKNOWN')
                error_msg = f"Error normalizando {code}: {e}"
                stats['errors'].append(error_msg)
                print(f"⚠️  {error_msg}")
        
        print(f"✅ Normalizados {len(normalized_countries)} países")
        
        # =====================================================================
        # FASE 2: LOAD SA - Cargar a Staging Area
        # =====================================================================
        print(f"\n💾 FASE 2: LOAD SA - Cargando a tabla Staging")
        print("-" * 80)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # TRUNCATE SA
        print(f"🗑️  Truncando ga_integration.sa_training_countries...")
        cursor.execute("TRUNCATE TABLE ga_integration.sa_training_countries")
        stats['sa_truncated'] = True
        
        # INSERT en SA
        if normalized_countries:
            print(f"📝 Insertando {len(normalized_countries)} países en SA...")
            insert_countries_sa(cursor, normalized_countries)
            stats['sa_loaded'] = len(normalized_countries)
            print(f"✅ {stats['sa_loaded']} países cargados en SA")
        
        conn.commit()
        
        # =====================================================================
        # FASE 3: MERGE TH - Consolidar en Tabla Histórica
        # =====================================================================
        print(f"\n🔄 FASE 3: MERGE TH - Consolidando SA → TH")
        print("-" * 80)
        
        merge_sql = """
        WITH upsert AS (
            INSERT INTO ga_integration.th_training_countries (
                code_iso2, code_iso3, code_numeric,
                name_common, name_official, name_native,
                capital, region, subregion,
                latitude, longitude, area, landlocked,
                population,
                languages, currencies,
                timezones, borders, flag_emoji, flag_svg,
                independent, un_member,
                first_loaded_at, last_updated_at, version
            )
            SELECT 
                code_iso2, code_iso3, code_numeric,
                name_common, name_official, name_native,
                capital, region, subregion,
                latitude, longitude, area, landlocked,
                population,
                languages, currencies,
                timezones, borders, flag_emoji, flag_svg,
                independent, un_member,
                NOW(), NOW(), 1
            FROM ga_integration.sa_training_countries
            ON CONFLICT (code_iso3)
            DO UPDATE SET
                code_iso2 = EXCLUDED.code_iso2,
                code_numeric = EXCLUDED.code_numeric,
                name_common = EXCLUDED.name_common,
                name_official = EXCLUDED.name_official,
                name_native = EXCLUDED.name_native,
                capital = EXCLUDED.capital,
                region = EXCLUDED.region,
                subregion = EXCLUDED.subregion,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                area = EXCLUDED.area,
                landlocked = EXCLUDED.landlocked,
                population = EXCLUDED.population,
                languages = EXCLUDED.languages,
                currencies = EXCLUDED.currencies,
                timezones = EXCLUDED.timezones,
                borders = EXCLUDED.borders,
                flag_emoji = EXCLUDED.flag_emoji,
                flag_svg = EXCLUDED.flag_svg,
                independent = EXCLUDED.independent,
                un_member = EXCLUDED.un_member,
                last_updated_at = NOW(),
                version = ga_integration.th_training_countries.version + 1
            RETURNING 
                CASE WHEN version = 1 THEN 'INSERT' ELSE 'UPDATE' END as operation
        )
        SELECT 
            COUNT(*) FILTER (WHERE operation = 'INSERT') as inserts,
            COUNT(*) FILTER (WHERE operation = 'UPDATE') as updates
        FROM upsert;
        """
        
        cursor.execute(merge_sql)
        result = cursor.fetchone()
        stats['th_inserted'] = result[0] or 0
        stats['th_updated'] = result[1] or 0
        
        print(f"✅ MERGE completado:")
        print(f"   📥 Insertados: {stats['th_inserted']}")
        print(f"   🔄 Actualizados: {stats['th_updated']}")
        
        # Total en TH
        cursor.execute("SELECT COUNT(*) FROM ga_integration.th_training_countries")
        stats['th_total'] = cursor.fetchone()[0]
        print(f"   📊 Total en TH: {stats['th_total']}")
        
        conn.commit()
        stats['success'] = True
        
    except Exception as e:
        error_msg = f"Error crítico: {e}"
        stats['errors'].append(error_msg)
        print(f"\n❌ {error_msg}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    
    finally:
        if conn:
            conn.close()
    
    # =========================================================================
    # RESUMEN
    # =========================================================================
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    stats['end_time'] = end_time
    stats['duration_seconds'] = duration
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN ETL: Get All Countries")
    print(f"{'='*80}")
    print(f"   Execution ID: {execution_id}")
    print(f"   Estado: {'✅ EXITOSO' if stats['success'] else '❌ FALLIDO'}")
    print(f"   Duración: {duration:.2f} segundos")
    print(f"\n   📥 EXTRACT:")
    print(f"      API Calls: {stats['api_calls']}")
    print(f"      Países extraídos: {stats['countries_extracted']}")
    print(f"\n   💾 LOAD SA:")
    print(f"      Truncado: {'✅' if stats['sa_truncated'] else '❌'}")
    print(f"      Cargados: {stats['sa_loaded']}")
    print(f"\n   🔄 MERGE TH:")
    print(f"      Insertados: {stats['th_inserted']}")
    print(f"      Actualizados: {stats['th_updated']}")
    print(f"      Total en TH: {stats['th_total']}")
    
    if stats['errors']:
        print(f"\n   ⚠️  Errores: {len(stats['errors'])}")
        for err in stats['errors'][:5]:
            print(f"      - {err}")
    
    print(f"{'='*80}\n")
    
    return stats


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def insert_countries_sa(cursor, countries: List[Dict[str, Any]]):
    """Inserta países en SA usando bulk insert"""
    columns = [
        'code_iso2', 'code_iso3', 'code_numeric',
        'name_common', 'name_official', 'name_native',
        'capital', 'region', 'subregion',
        'latitude', 'longitude', 'area', 'landlocked',
        'population',
        'languages', 'currencies',
        'timezones', 'borders', 'flag_emoji', 'flag_svg',
        'independent', 'un_member',
        'execution_id', 'loaded_at',
    ]
    
    values = [
        tuple(country.get(col) for col in columns)
        for country in countries
    ]
    
    cols_str = ', '.join(columns)
    sql = f"INSERT INTO ga_integration.sa_training_countries ({cols_str}) VALUES %s"
    
    execute_values(cursor, sql, values)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    print("\n" + "🌍" * 40)
    print("Script de prueba: Get All Countries (ETL completo)")
    print("🌍" * 40 + "\n")
    
    stats = etl_get_all_countries()
    
    if stats['success']:
        print(f"\n✅ ETL ejecutado exitosamente")
        print(f"\n💡 Consultas útiles:")
        print(f"   -- Ver datos en TH:")
        print(f"   SELECT code_iso3, name_common, region, population FROM ga_integration.th_training_countries LIMIT 10;")
        print(f"\n   -- Ver países recién insertados:")
        print(f"   SELECT * FROM ga_integration.th_training_countries WHERE version = 1 LIMIT 10;")
        print(f"\n   -- Ver países actualizados:")
        print(f"   SELECT * FROM ga_integration.th_training_countries WHERE version > 1 ORDER BY last_updated_at DESC;")
    else:
        print(f"\n❌ ETL falló. Ver errores arriba.")
        sys.exit(1)
