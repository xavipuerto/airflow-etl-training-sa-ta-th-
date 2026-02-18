#!/usr/bin/env python3
"""
ETL: Get Countries - Campos CULTURALES/POLÍTICOS
=================================================

**Objetivo Funcional:** Cargar campos culturales, económicos y políticos de todos los países

**Endpoints:**
- GET /all?fields=cca2,cca3,languages,currencies,timezones,flags,independent,unMember,ccn3

**Flujo ETL:**
1. EXTRACT: Llamar API REST Countries (9 campos)
2. TRANSFORM: Normalizar idiomas, monedas, zonas horarias
3. LOAD SA: TRUNCATE + INSERT en sa_training_countries_culture

**Tabla destino:** ga_integration.sa_training_countries_culture

Este es el PASO 3 de 4 para cargar todos los datos de países.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import execute_batch

sys.path.insert(0, '/opt/airflow/scripts')
from training_rest_countries_client import RestCountriesClient


DB_CONFIG = {
    'host': 'postgres-goaigua',
    'port': 5432,
    'database': 'goaigua_data',
    'user': 'goaigua',
    'password': 'goaigua2026',
}


def extract_countries_culture() -> List[Dict[str, Any]]:
    """EXTRACT: Obtiene campos culturales/políticos de todos los países"""
    print("=" * 80)
    print("📥 EXTRACT: Obteniendo campos culturales/políticos de países...")
    print("=" * 80)
    
    client = RestCountriesClient()
    result = client.get_all_countries_culture()
    
    print(f"\n{result}")
    
    if not result.ok:
        raise Exception(f"Error en API: {result.status} - {result.text}")
    
    countries = result.json_obj
    print(f"✅ Se obtuvieron {len(countries)} países")
    print(f"   Campos esperados: cca2, cca3, languages, currencies, timezones, flags, independent, unMember, ccn3")
    
    return countries


def transform_country_culture(country: Dict[str, Any]) -> Dict[str, Any]:
    """TRANSFORM: Normaliza campos culturales/políticos de un país"""
    return {
        'code_iso2': country.get('cca2'),
        'code_iso3': country.get('cca3'),
        'code_numeric': country.get('ccn3'),
        'languages': json.dumps(country.get('languages', {})),
        'currencies': json.dumps(country.get('currencies', {})),
        'timezones': json.dumps(country.get('timezones', [])),
        'flag_emoji': country.get('flag'),
        'flag_svg': country.get('flags', {}).get('svg'),
        'independent': country.get('independent'),
        'un_member': country.get('unMember'),
    }


def transform_countries_culture(countries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """TRANSFORM: Normaliza todos los países"""
    print("\n" + "=" * 80)
    print("🔄 TRANSFORM: Normalizando campos culturales/políticos...")
    print("=" * 80)
    
    transformed = []
    for country in countries:
        try:
            data = transform_country_culture(country)
            transformed.append(data)
        except Exception as e:
            code = country.get('cca3', 'UNKNOWN')
            print(f"⚠️  Error transformando {code}: {e}")
            continue
    
    print(f"✅ Se transformaron {len(transformed)} países")
    
    # Mostrar ejemplo con idiomas
    example_with_lang = next((c for c in transformed if c.get('languages') and c['languages'] != '{}'), None)
    if example_with_lang:
        print(f"   Ejemplo con idiomas: {example_with_lang['code_iso3']}")
        print(f"                        Independiente: {example_with_lang['independent']}")
        print(f"                        ONU: {example_with_lang['un_member']}")
    
    return transformed


def load_to_sa(countries: List[Dict[str, Any]], execution_id: str) -> int:
    """LOAD SA: Carga datos en Staging Area (TRUNCATE + INSERT)"""
    print("\n" + "=" * 80)
    print(f"📤 LOAD SA: Cargando en ga_integration.sa_training_countries_culture...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # TRUNCATE
        print(f"🗑️  TRUNCATE ga_integration.sa_training_countries_culture")
        cur.execute("TRUNCATE TABLE ga_integration.sa_training_countries_culture")
        
        # INSERT
        insert_sql = """
        INSERT INTO ga_integration.sa_training_countries_culture (
            code_iso2, code_iso3, code_numeric,
            languages, currencies, timezones,
            flag_emoji, flag_svg,
            independent, un_member,
            execution_id
        ) VALUES (
            %(code_iso2)s, %(code_iso3)s, %(code_numeric)s,
            %(languages)s, %(currencies)s, %(timezones)s,
            %(flag_emoji)s, %(flag_svg)s,
            %(independent)s, %(un_member)s,
            %(execution_id)s
        )
        """
        
        for country in countries:
            country['execution_id'] = execution_id
        
        print(f"📥 Insertando {len(countries)} países...")
        execute_batch(cur, insert_sql, countries, page_size=100)
        
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM ga_integration.sa_training_countries_culture")
        count = cur.fetchone()[0]
        
        print(f"✅ Se insertaron {count} países en SA")
        return count
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error en LOAD SA: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def etl_get_countries_culture(execution_id: str = None) -> Dict[str, Any]:
    """ETL Completo: Get Countries Culture"""
    if execution_id is None:
        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 80)
    print("🎭 ETL: GET COUNTRIES CULTURE")
    print("=" * 80)
    print(f"Execution ID: {execution_id}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 80)
    
    try:
        countries_raw = extract_countries_culture()
        countries_transformed = transform_countries_culture(countries_raw)
        rows_inserted = load_to_sa(countries_transformed, execution_id)
        
        stats = {
            'status': 'SUCCESS',
            'execution_id': execution_id,
            'countries_extracted': len(countries_raw),
            'countries_transformed': len(countries_transformed),
            'rows_inserted_sa': rows_inserted,
        }
        
        print("\n" + "=" * 80)
        print("✅ ETL COMPLETADO EXITOSAMENTE")
        print("=" * 80)
        print(f"Países extraídos: {stats['countries_extracted']}")
        print(f"Filas en SA: {stats['rows_inserted_sa']}")
        print("=" * 80)
        
        return stats
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ETL FALLIDO")
        print("=" * 80)
        print(f"Error: {e}")
        print("=" * 80)
        raise


if __name__ == '__main__':
    execution_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    stats = etl_get_countries_culture(execution_id=execution_id)
    print("\n📊 Resumen:")
    print(json.dumps(stats, indent=2))
