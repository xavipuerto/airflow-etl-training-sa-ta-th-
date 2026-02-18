#!/usr/bin/env python3
"""
ETL: Merge Countries - Combine SA → TH
======================================

**Functional Objective:** Combine 3 Staging Areas and perform MERGE into TH

**Sources:**
- ga_integration.sa_training_countries_basic (basic fields)
- ga_integration.sa_training_countries_geo (geographic fields)
- ga_integration.sa_training_countries_culture (cultural/political fields)

**ETL Flow:**
1. READ: Read the 3 SA with JOIN by code_iso3
2. TRANSFORM: Combine fields from the 3 sources
3. LOAD TH: MERGE (INSERT new, UPDATE existing)

**Target table:** ga_integration.th_training_countries

This is STEP 4 (FINAL) to load all country data.

🎯 ADVANCED ETL CONCEPT:
This script demonstrates how to combine multiple Staging Areas before loading to TH.
Useful pattern when:
- Source requires multiple calls (like REST Countries with 10-field limit)
- You have multiple complementary data sources
- You want to parallelize extraction but centralize MERGE
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import execute_batch

DB_CONFIG = {
    'host': 'postgres-goaigua',
    'port': 5432,
    'database': 'goaigua_data',
    'user': 'goaigua',
    'password': 'goaigua2026',
}


# =============================================================================
# READ SA
# =============================================================================

def read_combined_sa() -> List[Dict[str, Any]]:
    """
    READ: Read and combine the 3 Staging Areas with JOIN
    
    Executes a JOIN of the 3 SA using code_iso3 as key.
    
    Returns:
        List of countries with all combined fields
    """
    print("=" * 80)
    print("📖 READ SA: Reading and combining 3 Staging Areas...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # JOIN the 3 SA tables
        query = f"""
        SELECT
            -- From BASIC
            b.code_iso2,
            b.code_iso3,
            b.name_common,
            b.name_official,
            b.name_native,
            b.capital,
            b.region,
            b.subregion,
            b.area,
            b.population,
            
            -- From GEO
            g.latitude,
            g.longitude,
            g.landlocked,
            g.borders,
            
            -- From CULTURE
            c.code_numeric,
            c.languages,
            c.currencies,
            c.timezones,
            c.flag_emoji,
            c.flag_svg,
            c.independent,
            c.un_member
            
        FROM ga_integration.sa_training_countries_basic b
        INNER JOIN ga_integration.sa_training_countries_geo g ON b.code_iso3 = g.code_iso3
        INNER JOIN ga_integration.sa_training_countries_culture c ON b.code_iso3 = c.code_iso3
        ORDER BY b.code_iso3
        """
        
        print(f"🔍 Ejecutando JOIN de 3 tablas SA:")
        print(f"   - sa_training_countries_basic")
        print(f"   - sa_training_countries_geo")
        print(f"   - sa_training_countries_culture")
        
        cur.execute(query)
        rows = cur.fetchall()
        
        # Convertir a lista de dicts
        columns = [
            'code_iso2', 'code_iso3', 'name_common', 'name_official', 'name_native',
            'capital', 'region', 'subregion', 'area', 'population',
            'latitude', 'longitude', 'landlocked', 'borders',
            'code_numeric', 'languages', 'currencies', 'timezones',
            'flag_emoji', 'flag_svg', 'independent', 'un_member'
        ]
        
        countries = []
        for row in rows:
            country = dict(zip(columns, row))
            countries.append(country)
        
        print(f"✅ Combined {len(countries)} countries")
        print(f"   Total fields per country: {len(columns)}")
        
        if countries:
            example = countries[0]
            print(f"\n📋 Example: {example['name_common']} ({example['code_iso3']})")
            print(f"   Region: {example['region']}")
            print(f"   Population: {example['population']:,}" if example['population'] else "   Population: N/A")
            print(f"   Coordenadas: ({example['latitude']}, {example['longitude']})")
            print(f"   Landlocked: {example['landlocked']}")
            print(f"   Independiente: {example['independent']}")
        
        return countries
        
    finally:
        cur.close()
        conn.close()


# =============================================================================
# LOAD TH (MERGE)
# =============================================================================

def merge_to_th(countries: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    LOAD TH: MERGE datos combinados en tabla histórica
    
    Estrategia:
    - INSERT para países nuevos
    - UPDATE para países existentes (incrementa version)
    
    Args:
        countries: Lista de países con todos los campos
        
    Returns:
        Dict con estadísticas (inserted, updated)
    """
    print("\n" + "=" * 80)
    print(f"📤 LOAD TH: MERGE into ga_integration.th_training_countries...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # MERGE using INSERT ... ON CONFLICT DO UPDATE
        merge_sql = """
        INSERT INTO ga_integration.th_training_countries (
            code_iso2, code_iso3, code_numeric,
            name_common, name_official, name_native,
            capital, region, subregion,
            latitude, longitude, area, landlocked,
            population,
            languages, currencies, timezones, borders,
            flag_emoji, flag_svg,
            independent, un_member,
            first_loaded_at, last_updated_at, version
        ) VALUES (
            %(code_iso2)s, %(code_iso3)s, %(code_numeric)s,
            %(name_common)s, %(name_official)s, %(name_native)s,
            %(capital)s, %(region)s, %(subregion)s,
            %(latitude)s, %(longitude)s, %(area)s, %(landlocked)s,
            %(population)s,
            %(languages)s, %(currencies)s, %(timezones)s, %(borders)s,
            %(flag_emoji)s, %(flag_svg)s,
            %(independent)s, %(un_member)s,
            NOW(), NOW(), 1
        )
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
        """
        
        # Count records before
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_countries")
        count_before = cur.fetchone()[0]
        
        print(f"📊 Before MERGE: {count_before} countries in TH")
        print(f"🔄 Executing MERGE of {len(countries)} countries...")
        
        # Convert JSONB fields (dict/list) to JSON strings for psycopg2
        # psycopg2 reads JSONB columns as Python dict/list, but needs JSON strings for INSERT
        jsonb_fields = ['languages', 'currencies', 'timezones', 'borders']
        for country in countries:
            for field in jsonb_fields:
                value = country.get(field)
                if value is not None and not isinstance(value, str):
                    # Convert dict/list to JSON string
                    country[field] = json.dumps(value)
                elif value is None:
                    # Ensure None is properly handled
                    country[field] = None
        
        execute_batch(cur, merge_sql, countries, page_size=100)
        conn.commit()
        
        # Count after
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_countries")
        count_after = cur.fetchone()[0]
        
        # Calculate inserts and updates (approximate)
        inserted = max(0, count_after - count_before)
        updated = len(countries) - inserted
        
        print(f"\n✅ MERGE completed:")
        print(f"   Countries inserted (new): {inserted}")
        print(f"   Countries updated (existing): {updated}")
        print(f"   Total in TH: {count_after}")
        
        # Verify some countries with all fields
        cur.execute("""
            SELECT code_iso3, name_common, population, landlocked, 
                   languages IS NOT NULL as has_languages,
                   currencies IS NOT NULL as has_currencies
            FROM ga_integration.th_training_countries
            WHERE code_iso3 IN ('ESP', 'USA', 'CHN', 'JPN', 'DEU')
            ORDER BY code_iso3
        """)
        
        print(f"\n📋 Verificación de países de ejemplo:")
        for row in cur.fetchall():
            code, name, pop, landlocked, has_lang, has_curr = row
            print(f"   {code}: {name} - Pop: {pop:,} - Landlocked: {landlocked} - Lang: {has_lang} - Curr: {has_curr}")
        
        return {
            'inserted': inserted,
            'updated': updated,
            'total': count_after,
        }
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error in MERGE: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# =============================================================================
# ORQUESTACIÓN ETL
# =============================================================================

def etl_merge_countries_to_th(execution_id: str = None) -> Dict[str, Any]:
    """
    ETL Completo: Merge Countries SA → TH
    
    Args:
        execution_id: ID de ejecución (compartido con las 3 SA)
        
    Returns:
        Dict con estadísticas de la ejecución
    """
    if execution_id is None:
        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 80)
    print("🔗 ETL: MERGE COUNTRIES SA → TH")
    print("=" * 80)
    print(f"Execution ID: {execution_id}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 80)
    print("\n💡 Este proceso combina 3 Staging Areas en 1 tabla histórica:")
    print(f"   sa_training_countries_basic (básico)")
    print(f"   sa_training_countries_geo (geo)")
    print(f"   sa_training_countries_culture (culture)")
    print(f"   → th_training_countries (histórica)")
    print("=" * 80)
    
    try:
        # 1. READ: Leer y combinar SA
        countries = read_combined_sa()
        
        # 2. LOAD TH: MERGE
        merge_stats = merge_to_th(countries)
        
        # Statistics
        stats = {
            'status': 'SUCCESS',
            'execution_id': execution_id,
            'countries_combined': len(countries),
            'countries_inserted': merge_stats['inserted'],
            'countries_updated': merge_stats['updated'],
            'total_in_th': merge_stats['total'],
        }
        
        print("\n" + "=" * 80)
        print("✅ ETL COMPLETADO EXITOSAMENTE")
        print("=" * 80)
        print(f"Estado: {stats['status']}")
        print(f"Execution ID: {stats['execution_id']}")
        print(f"Países combinados: {stats['countries_combined']}")
        print(f"Nuevos insertados: {stats['countries_inserted']}")
        print(f"Existentes actualizados: {stats['countries_updated']}")
        print(f"Total en TH: {stats['total_in_th']}")
        print("=" * 80)
        
        return stats
        
    except Exception as e:
        print("\n" + "=" * 80)
        print(f"❌ ETL FAILED")
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
    stats = etl_merge_countries_to_th(execution_id=execution_id)
    
    print("\n📊 Resumen:")
    print(json.dumps(stats, indent=2))
