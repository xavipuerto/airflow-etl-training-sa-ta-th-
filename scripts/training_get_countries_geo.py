#!/usr/bin/env python3
"""
ETL: Get Countries - GEOGRAPHIC Fields
========================================

**Functional Objective:** Load geographic fields of all countries

**Endpoints:**
- GET /all?fields=cca2,cca3,latlng,landlocked,borders

**ETL Flow:**
1. EXTRACT: Call REST Countries API (5 fields)
2. TRANSFORM: Normalize coordinates and borders
3. LOAD SA: TRUNCATE + INSERT into sa_training_countries_geo

**Target table:** ga_integration.sa_training_countries_geo

This is STEP 2 of 4 to load all country data.
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


def extract_countries_geo() -> List[Dict[str, Any]]:
    """EXTRACT: Get geographic fields of all countries"""
    print("=" * 80)
    print("📥 EXTRACT: Getting geographic fields from countries...")
    print("=" * 80)
    
    client = RestCountriesClient()
    result = client.get_all_countries_geo()
    
    print(f"\n{result}")
    
    if not result.ok:
        raise Exception(f"API Error: {result.status} - {result.text}")
    
    countries = result.json_obj
    print(f"✅ Retrieved {len(countries)} countries")
    print(f"   Expected fields: cca2, cca3, latlng, landlocked, borders"))
    
    return countries


def transform_country_geo(country: Dict[str, Any]) -> Dict[str, Any]:
    """TRANSFORM: Normalize geographic fields of a country"""
    latlng = country.get('latlng', [])
    
    return {
        'code_iso2': country.get('cca2'),
        'code_iso3': country.get('cca3'),
        'latitude': latlng[0] if len(latlng) > 0 else None,
        'longitude': latlng[1] if len(latlng) > 1 else None,
        'landlocked': country.get('landlocked'),
        'borders': json.dumps(country.get('borders', [])),
    }


def transform_countries_geo(countries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """TRANSFORM: Normalize all countries"""
    print("\n" + "=" * 80)
    print("🔄 TRANSFORM: Normalizing geographic fields...")
    print("=" * 80)
    
    transformed = []
    for country in countries:
        try:
            data = transform_country_geo(country)
            transformed.append(data)
        except Exception as e:
            code = country.get('cca3', 'UNKNOWN')
            print(f"⚠️  Error transforming {code}: {e}")
            continue
    
    print(f"✅ Transformed {len(transformed)} countries")
    
    # Show example of landlocked country
    landlocked_example = next((c for c in transformed if c.get('landlocked')), None)
    if landlocked_example:
        print(f"   Example landlocked: {landlocked_example['code_iso3']}")
        print(f"                       Coordinates: ({landlocked_example['latitude']}, {landlocked_example['longitude']})"))
    
    return transformed


def load_to_sa(countries: List[Dict[str, Any]], execution_id: str) -> int:
    """LOAD SA: Load data into Staging Area (TRUNCATE + INSERT)"""
    print("\n" + "=" * 80)
    print(f"📤 LOAD SA: Loading into ga_integration.sa_training_countries_geo...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # TRUNCATE
        print(f"🗑️  TRUNCATE ga_integration.sa_training_countries_geo")
        cur.execute("TRUNCATE TABLE ga_integration.sa_training_countries_geo")
        
        # INSERT
        insert_sql = """
        INSERT INTO ga_integration.sa_training_countries_geo (
            code_iso2, code_iso3,
            latitude, longitude, landlocked, borders,
            execution_id
        ) VALUES (
            %(code_iso2)s, %(code_iso3)s,
            %(latitude)s, %(longitude)s, %(landlocked)s, %(borders)s,
            %(execution_id)s
        )
        """
        
        for country in countries:
            country['execution_id'] = execution_id
        
        print(f"📥 Inserting {len(countries)} countries...")
        execute_batch(cur, insert_sql, countries, page_size=100)
        
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM ga_integration.sa_training_countries_geo")
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


def etl_get_countries_geo(execution_id: str = None) -> Dict[str, Any]:
    """Complete ETL: Get Countries Geo"""
    if execution_id is None:
        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 80)
    print("🗺️  ETL: GET COUNTRIES GEO")
    print("=" * 80)
    print(f"Execution ID: {execution_id}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 80)
    
    try:
        countries_raw = extract_countries_geo()
        countries_transformed = transform_countries_geo(countries_raw)
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
    stats = etl_get_countries_geo(execution_id=execution_id)
    print("\n📊 Summary:")
    print(json.dumps(stats, indent=2))
