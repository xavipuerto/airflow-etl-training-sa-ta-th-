#!/usr/bin/env python3
"""
Script Funcional: Obtener países por REGIÓN + Estadísticas
===========================================================

Endpoint: GET /region/{region}
Objetivo: Extraer países de una región específica y calcular estadísticas

Regiones disponibles:
- africa
- americas
- asia
- europe
- oceania

Flujo completo SA → TH:
1. EXTRACT: Llamar API /region/{region} para cada región
2. LOAD SA: TRUNCATE + INSERT en sa_training_regions_stats
3. MERGE TH: UPSERT de SA a th_training_regions_stats

Este script demuestra:
- Llamadas múltiples a la misma API con parámetros diferentes
- Agregación de datos (COUNT, SUM, AVG)
- Transformaciones complejas
"""
import sys
import psycopg2
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
TABLE_SA = 'sa_training_regions_stats'
TABLE_TH = 'th_training_regions_stats'

# Regiones disponibles en REST Countries API
REGIONS = ['africa', 'americas', 'asia', 'europe', 'oceania']


# =============================================================================
# FUNCIÓN PRINCIPAL: ETL COMPLETO
# =============================================================================

def etl_get_regions_stats(execution_id: str = None, regions: List[str] = None) -> Dict[str, Any]:
    """
    ETL completo: GET /region/{region} → Agregación → SA → TH
    
    Args:
        execution_id: ID de la ejecución
        regions: Lista de regiones a procesar (default: todas)
        
    Returns:
        Dict con estadísticas completas
    """
    if not execution_id:
        execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if not regions:
        regions = REGIONS
    
    start_time = datetime.now()
    print(f"\n{'='*80}")
    print(f"📊 ETL: Estadísticas por Región")
    print(f"   Endpoint: GET /region/{{region}}")
    print(f"   Regiones: {', '.join(regions)}")
    print(f"   Execution ID: {execution_id}")
    print(f"   Timestamp: {start_time}")
    print(f"{'='*80}\n")
    
    stats = {
        'execution_id': execution_id,
        'start_time': start_time,
        'endpoint': '/region/{region}',
        'regions_processed': [],
        
        # Fase EXTRACT
        'api_calls': 0,
        'countries_extracted': 0,
        
        # Fase TRANSFORM
        'regions_calculated': 0,
        
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
    region_stats_list = []
    
    try:
        # =====================================================================
        # FASE 1: EXTRACT - Obtener países por región
        # =====================================================================
        print("📥 FASE 1: EXTRACT - Obteniendo países por región")
        print("-" * 80)
        
        client = RestCountriesClient()
        
        for region in regions:
            print(f"\n🌍 Procesando región: {region.upper()}")
            result = client.get_countries_by_region(region)
            stats['api_calls'] += 1
            
            print(f"   {result}")
            
            if not result.ok:
                error_msg = f"Error en API para región {region}: {result.status}"
                stats['errors'].append(error_msg)
                print(f"   ⚠️  {error_msg}")
                continue
            
            countries = result.json_obj
            print(f"   ✅ Obtenidos {len(countries)} países de {region}")
            
            # ================================================================
            # FASE 2: TRANSFORM - Calcular estadísticas de la región
            # ================================================================
            print(f"   📊 Calculando estadísticas...")
            
            region_stats = calculate_region_stats(region, countries, execution_id)
            region_stats_list.append(region_stats)
            
            stats['countries_extracted'] += len(countries)
            stats['regions_processed'].append(region)
            
            print(f"   ✅ Estadísticas calculadas para {region}:")
            print(f"      Total países: {region_stats['total_countries']}")
            print(f"      Población total: {region_stats['total_population']:,}")
            print(f"      Población promedio: {region_stats['avg_population']:,.0f}")
        
        stats['regions_calculated'] = len(region_stats_list)
        
        print(f"\n✅ Procesadas {stats['regions_calculated']} regiones")
        print(f"✅ Total países extraídos: {stats['countries_extracted']}")
        
        # =====================================================================
        # FASE 3: LOAD SA - Cargar estadísticas a Staging Area
        # =====================================================================
        print(f"\n💾 FASE 3: LOAD SA - Cargando estadísticas a tabla Staging")
        print("-" * 80)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # TRUNCATE SA
        print(f"🗑️  Truncando ga_integration.sa_training_regions_stats...")
        cursor.execute("TRUNCATE TABLE ga_integration.sa_training_regions_stats")
        stats['sa_truncated'] = True
        
        # INSERT en SA
        if region_stats_list:
            print(f"📝 Insertando {len(region_stats_list)} registros de regiones en SA...")
            insert_stats_sa(cursor, region_stats_list)
            stats['sa_loaded'] = len(region_stats_list)
            print(f"✅ {stats['sa_loaded']} regiones cargadas en SA")
        
        conn.commit()
        
        # =====================================================================
        # FASE 4: MERGE TH - Consolidar en Tabla Histórica
        # =====================================================================
        print(f"\n🔄 FASE 4: MERGE TH - Consolidando SA → TH")
        print("-" * 80)
        
        merge_sql = """
        WITH upsert AS (
            INSERT INTO ga_integration.th_training_regions_stats (
                region,
                total_countries,
                total_population,
                avg_population,
                total_area,
                landlocked_count,
                independent_count,
                un_member_count,
                first_loaded_at,
                last_updated_at,
                version
            )
            SELECT 
                region,
                total_countries,
                total_population,
                avg_population,
                total_area,
                landlocked_count,
                independent_count,
                un_member_count,
                NOW(),
                NOW(),
                1
            FROM ga_integration.sa_training_regions_stats
            ON CONFLICT (region)
            DO UPDATE SET
                total_countries = EXCLUDED.total_countries,
                total_population = EXCLUDED.total_population,
                avg_population = EXCLUDED.avg_population,
                total_area = EXCLUDED.total_area,
                landlocked_count = EXCLUDED.landlocked_count,
                independent_count = EXCLUDED.independent_count,
                un_member_count = EXCLUDED.un_member_count,
                last_updated_at = NOW(),
                version = ga_integration.th_training_regions_stats.version + 1
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
        print(f"   📥 Insertadas: {stats['th_inserted']} regiones")
        print(f"   🔄 Actualizadas: {stats['th_updated']} regiones")
        
        # Total en TH
        cursor.execute("SELECT COUNT(*) FROM ga_integration.th_training_regions_stats")
        stats['th_total'] = cursor.fetchone()[0]
        print(f"   📊 Total en TH: {stats['th_total']} regiones")
        
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
    print(f"📊 RESUMEN ETL: Regions Statistics")
    print(f"{'='*80}")
    print(f"   Execution ID: {execution_id}")
    print(f"   Estado: {'✅ EXITOSO' if stats['success'] else '❌ FALLIDO'}")
    print(f"   Duración: {duration:.2f} segundos")
    print(f"\n   📥 EXTRACT:")
    print(f"      API Calls: {stats['api_calls']}")
    print(f"      Regiones procesadas: {', '.join(stats['regions_processed'])}")
    print(f"      Países extraídos: {stats['countries_extracted']}")
    print(f"\n   📊 TRANSFORM:")
    print(f"      Regiones calculadas: {stats['regions_calculated']}")
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

def calculate_region_stats(region: str, countries: List[Dict], execution_id: str) -> Dict[str, Any]:
    """
    Calcula estadísticas agregadas de una región
    
    Este es un ejemplo de TRANSFORM en ETL:
    - Agregaciones (COUNT, SUM, AVG)
    - Filtros condicionales
    - Cálculos derivados
    """
    total_countries = len(countries)
    total_population = 0
    total_area = 0
    landlocked_count = 0
    independent_count = 0
    un_member_count = 0
    
    for country in countries:
        # Población
        pop = country.get('population', 0)
        if pop:
            total_population += pop
        
        # Área
        area = country.get('area', 0)
        if area:
            total_area += area
        
        # Landlocked (sin salida al mar)
        if country.get('landlocked', False):
            landlocked_count += 1
        
        # Independiente
        if country.get('independent', False):
            independent_count += 1
        
        # Miembro ONU
        if country.get('unMember', False):
            un_member_count += 1
    
    # Promedio
    avg_population = total_population / total_countries if total_countries > 0 else 0
    
    return {
        'region': region,
        'total_countries': total_countries,
        'total_population': total_population,
        'avg_population': round(avg_population, 2),
        'total_area': round(total_area, 2),
        'landlocked_count': landlocked_count,
        'independent_count': independent_count,
        'un_member_count': un_member_count,
        'execution_id': execution_id,
        'loaded_at': datetime.now(),
    }


def insert_stats_sa(cursor, stats_list: List[Dict[str, Any]]):
    """Inserta estadísticas en SA"""
    for stats in stats_list:
        sql = """
        INSERT INTO ga_integration.sa_training_regions_stats (
            region, total_countries, total_population, avg_population,
            total_area, landlocked_count, independent_count, un_member_count,
            execution_id, loaded_at
        ) VALUES (
            %(region)s, %(total_countries)s, %(total_population)s, %(avg_population)s,
            %(total_area)s, %(landlocked_count)s, %(independent_count)s, %(un_member_count)s,
            %(execution_id)s, %(loaded_at)s
        )
        """
        cursor.execute(sql, stats)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    print("\n" + "📊" * 40)
    print("Script de prueba: Get Regions Stats (ETL completo)")
    print("📊" * 40 + "\n")
    
    stats = etl_get_regions_stats()
    
    if stats['success']:
        print(f"\n✅ ETL ejecutado exitosamente")
        print(f"\n💡 Consultas útiles:")
        print(f"   -- Ver estadísticas de todas las regiones:")
        print(f"   SELECT * FROM ga_integration.th_training_regions_stats ORDER BY total_population DESC;")
        print(f"\n   -- Región con más países:")
        print(f"   SELECT region, total_countries FROM ga_integration.th_training_regions_stats ORDER BY total_countries DESC LIMIT 1;")
        print(f"\n   -- Región con mayor población:")
        print(f"   SELECT region, total_population FROM ga_integration.th_training_regions_stats ORDER BY total_population DESC LIMIT 1;")
        print(f"\n   -- Comparar versiones (si ya se ejecutó antes):")
        print(f"   SELECT region, version, last_updated_at FROM ga_integration.th_training_regions_stats ORDER BY last_updated_at DESC;")
    else:
        print(f"\n❌ ETL falló. Ver errores arriba.")
        sys.exit(1)
