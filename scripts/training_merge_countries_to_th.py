#!/usr/bin/env python3
"""
ETL: Merge Countries - Combinar SA → TH
=======================================

**Objetivo Funcional:** Combinar 3 Staging Areas y hacer MERGE en TH

**Fuentes:**
- ga_integration.sa_training_countries_basic (campos básicos)
- ga_integration.sa_training_countries_geo (campos geográficos)
- ga_integration.sa_training_countries_culture (campos culturales/políticos)

**Flujo ETL:**
1. READ: Leer las 3 SA con JOIN por code_iso3
2. TRANSFORM: Combinar campos de las 3 fuentes
3. LOAD TH: MERGE (INSERT nuevos, UPDATE existentes)

**Tabla destino:** ga_integration.th_training_countries

Este es el PASO 4 (FINAL) para cargar todos los datos de países.

🎯 CONCEPTO ETL AVANZADO:
Este script demuestra cómo combinar múltiples Staging Areas antes de cargar a TH.
Patrón útil cuando:
- La fuente requiere múltiples llamadas (como REST Countries con límite de 10 campos)
- Tienes múltiples fuentes de datos complementarias
- Quieres paralelizar la extracción pero centralizar el MERGE
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
    READ: Lee y combina las 3 Staging Areas con JOIN
    
    Ejecuta un JOIN de las 3 SA usando code_iso3 como clave.
    
    Returns:
        Lista de países con todos los campos combinados
    """
    print("=" * 80)
    print("📖 READ SA: Leyendo y combinando 3 Staging Areas...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # JOIN de las 3 SA
        query = f"""
        SELECT
            -- De BASIC
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
            
            -- De GEO
            g.latitude,
            g.longitude,
            g.landlocked,
            g.borders,
            
            -- De CULTURE
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
        
        print(f"✅ Se combinaron {len(countries)} países")
        print(f"   Total de campos por país: {len(columns)}")
        
        if countries:
            example = countries[0]
            print(f"\n📋 Ejemplo: {example['name_common']} ({example['code_iso3']})")
            print(f"   Región: {example['region']}")
            print(f"   Población: {example['population']:,}" if example['population'] else "   Población: N/A")
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
    print(f"📤 LOAD TH: MERGE en ga_integration.th_training_countries...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # MERGE usando INSERT ... ON CONFLICT DO UPDATE
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
        
        # Contar registros antes
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_countries")
        count_before = cur.fetchone()[0]
        
        print(f"📊 Antes del MERGE: {count_before} países en TH")
        print(f"🔄 Ejecutando MERGE de {len(countries)} países...")
        
        execute_batch(cur, merge_sql, countries, page_size=100)
        conn.commit()
        
        # Contar después
        cur.execute("SELECT COUNT(*) FROM ga_integration.th_training_countries")
        count_after = cur.fetchone()[0]
        
        # Calcular inserts y updates (aproximado)
        inserted = max(0, count_after - count_before)
        updated = len(countries) - inserted
        
        print(f"\n✅ MERGE completado:")
        print(f"   Países insertados (nuevos): {inserted}")
        print(f"   Países actualizados (existentes): {updated}")
        print(f"   Total en TH: {count_after}")
        
        # Verificar algunos países con todos los campos
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
        print(f"❌ Error en MERGE: {e}")
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
        
        # Estadísticas
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
    stats = etl_merge_countries_to_th(execution_id=execution_id)
    
    print("\n📊 Resumen:")
    print(json.dumps(stats, indent=2))
