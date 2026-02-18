#!/usr/bin/env python3
"""
ETL: Get Countries - CULTURAL/POLITICAL Fields
=================================================

**Functional Objective:** Load cultural, economic and political fields of all countries

**Endpoints:**
- GET /all?fields=cca2,cca3,languages,currencies,timezones,flags,independent,unMember,ccn3

**ETL Flow:**
1. EXTRACT: Call REST Countries API (9 fields)
2. TRANSFORM: Normalize languages, currencies, timezones
3. LOAD SA: TRUNCATE + INSERT into sa_training_countries_culture

**Target table:** ga_integration.sa_training_countries_culture

This is STEP 3 of 4 to load all country data.
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
    """EXTRACT: Get cultural/political fields of all countries"""
    print("=" * 80)
    print("📥 EXTRACT: Getting cultural/political fields from countries...")
    print("=" * 80)
    
    client = RestCountriesClient()
    result = client.get_all_countries_culture()
    
    print(f"\n{result}")
    
    if not result.ok:
        raise Exception(f"API Error: {result.status} - {result.text}")
    
    countries = result.json_obj
    print(f"✅ Retrieved {len(countries)} countries")
    print(f"   Expected fields: cca2, cca3, languages, currencies, timezones, flags, independent, unMember, ccn3"))
    
    return countries


def transform_country_culture(country: Dict[str, Any]) -> Dict[str, Any]:
    """TRANSFORM: Normalize cultural/political fields of a country"""
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
    """TRANSFORM: Normalize all countries"""
    print("\n" + "=" * 80)
    print("🔄 TRANSFORM: Normalizing cultural/political fields...")
    print("=" * 80)
    
    transformed = []
    for country in countries:
        try:
            data = transform_country_culture(country)
            transformed.append(data)
        except Exception as e:
            code = country.get('cca3', 'UNKNOWN')
            print(f"⚠️  Error transforming {code}: {e}")
            continue
    
    print(f"✅ Transformed {len(transformed)} countries")
    
    # Show example with languages
    example_with_lang = next((c for c in transformed if c.get('languages') and c['languages'] != '{}'), None)
    if example_with_lang:
        print(f"   Example with languages: {example_with_lang['code_iso3']}")
        print(f"                           Independent: {example_with_lang['independent']}")
        print(f"                           UN: {example_with_lang['un_member']}"))
    
    return transformed


def load_to_sa(countries: List[Dict[str, Any]], execution_id: str) -> int:
    """LOAD SA: Load data into Staging Area (TRUNCATE + INSERT)"""
    print("\n" + "=" * 80)
    print(f"📤 LOAD SA: Loading into ga_integration.sa_training_countries_culture...")
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
        
        print(f"📥 Inserting {len(countries)} countries...")
        execute_batch(cur, insert_sql, countries, page_size=100)
        
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM ga_integration.sa_training_countries_culture")
        count = cur.fetchone()[0]
        
        print(f"✅ Inserted {count} countries into SA")
        return count
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error in LOAD SA: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def etl_get_countries_culture(execution_id: str = None) -> Dict[str, Any]:
    """Complete ETL: Get Countries Culture"""
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
        print("✅ ETL COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"Countries extracted: {stats['countries_extracted']}")
        print(f"Rows in SA: {stats['rows_inserted_sa']}")
        print("=" * 80)
        
        return stats
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ETL FAILED")
        print("=" * 80)
        print(f"Error: {e}")
        print("=" * 80)
        raise


if __name__ == '__main__':
    execution_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    stats = etl_get_countries_culture(execution_id=execution_id)
    print("\n📊 Summary:")
    print(json.dumps(stats, indent=2))
