#!/usr/bin/env python3
"""
ETL: Get Countries - BASIC Fields
==================================

**Functional Objective:** Load basic fields of all countries

**Endpoints:**
- GET /all?fields=cca2,cca3,name,capital,region,subregion,area,population

**ETL Flow:**
1. EXTRACT: Call REST Countries API (8 fields)
2. TRANSFORM: Normalize names and structures
3. LOAD SA: TRUNCATE + INSERT into sa_training_countries_basic

**Target table:** ga_integration.sa_training_countries_basic

This is STEP 1 of 4 to load all country data:
- Step 1: Load basic fields (this script)
- Step 2: Load geographic fields
- Step 3: Load cultural fields
- Step 4: MERGE the 3 SA into TH
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import execute_batch

# Import REST Countries client
sys.path.insert(0, '/opt/airflow/scripts')
from training_rest_countries_client import RestCountriesClient


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


# =============================================================================
# EXTRACT
# =============================================================================

def extract_countries_basic() -> List[Dict[str, Any]]:
    """
    EXTRACT: Get basic fields of all countries from the API
    
    Returns:
        List of dictionaries with country data (basic fields)
    """
    print("=" * 80)
    print("📥 EXTRACT: Getting basic fields from countries...")
    print("=" * 80)
    
    client = RestCountriesClient()
    result = client.get_all_countries_basic()
    
    print(f"\n{result}")
    
    if not result.ok:
        raise Exception(f"API Error: {result.status} - {result.text}")
    
    countries = result.json_obj
    print(f"✅ Retrieved {len(countries)} countries")
    print(f"   Expected fields: cca2, cca3, name, capital, region, subregion, area, population")
    
    return countries


# =============================================================================
# TRANSFORM
# =============================================================================

def transform_country_basic(country: Dict[str, Any]) -> Dict[str, Any]:
    """
    TRANSFORM: Normalize basic fields of a country
    
    Args:
        country: JSON object of the country from the API
        
    Returns:
        Dict with normalized data for SA
    """
    return {
        # Identifiers
        'code_iso2': country.get('cca2'),
        'code_iso3': country.get('cca3'),
        
        # Names
        'name_common': country.get('name', {}).get('common'),
        'name_official': country.get('name', {}).get('official'),
        'name_native': json.dumps(country.get('name', {}).get('nativeName', {})),
        
        # Basic geography
        'capital': json.dumps(country.get('capital', [])),
        'region': country.get('region'),
        'subregion': country.get('subregion'),
        'area': country.get('area'),
        
        # Population
        'population': country.get('population'),
    }


def transform_countries_basic(countries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    TRANSFORM: Normalize all countries
    
    Args:
        countries: List of countries from the API
        
    Returns:
        List of transformed countries
    """
    print("\n" + "=" * 80)
    print("🔄 TRANSFORM: Normalizing basic fields...")
    print("=" * 80)
    
    transformed = []
    for country in countries:
        try:
            data = transform_country_basic(country)
            transformed.append(data)
        except Exception as e:
            code = country.get('cca3', 'UNKNOWN')
            print(f"⚠️  Error transformando {code}: {e}")
            continue
    
    print(f"✅ Transformed {len(transformed)} countries")
    print(f"   Example: {transformed[0]['name_common']} ({transformed[0]['code_iso3']})")
    print(f"            Region: {transformed[0]['region']}, Population: {transformed[0]['population']:,}")
    
    return transformed


# =============================================================================
# LOAD SA
# =============================================================================

def load_to_sa(countries: List[Dict[str, Any]], execution_id: str) -> int:
    """
    LOAD SA: Load data into Staging Area (TRUNCATE + INSERT)
    
    Args:
        countries: List of transformed countries
        execution_id: Unique execution ID
        
    Returns:
        Number of rows inserted
    """
    print("\n" + "=" * 80)
    print(f"📤 LOAD SA: Loading into ga_integration.sa_training_countries_basic...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # TRUNCATE: Clean SA table
        print(f"🗑️  TRUNCATE ga_integration.sa_training_countries_basic")
        cur.execute("TRUNCATE TABLE ga_integration.sa_training_countries_basic")
        
        # INSERT: Load new data
        insert_sql = """
        INSERT INTO ga_integration.sa_training_countries_basic (
            code_iso2, code_iso3,
            name_common, name_official, name_native,
            capital, region, subregion, area, population,
            execution_id
        ) VALUES (
            %(code_iso2)s, %(code_iso3)s,
            %(name_common)s, %(name_official)s, %(name_native)s,
            %(capital)s, %(region)s, %(subregion)s, %(area)s, %(population)s,
            %(execution_id)s
        )
        """
        
        # Add execution_id to each record
        for country in countries:
            country['execution_id'] = execution_id
        
        print(f"📥 Inserting {len(countries)} countries...")
        execute_batch(cur, insert_sql, countries, page_size=100)
        
        conn.commit()
        
        # Verify
        cur.execute("SELECT COUNT(*) FROM ga_integration.sa_training_countries_basic")
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


# =============================================================================
# ETL ORCHESTRATION
# =============================================================================

def etl_get_countries_basic(execution_id: str = None) -> Dict[str, Any]:
    """
    Complete ETL: Get Countries Basic
    
    Args:
        execution_id: Execution ID (optional, generated if not provided)
        
    Returns:
        Dict with execution statistics
    """
    if execution_id is None:
        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "=" * 80)
    print("🌍 ETL: GET COUNTRIES BASIC")
    print("=" * 80)
    print(f"Execution ID: {execution_id}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 80)
    
    try:
        # 1. EXTRACT
        countries_raw = extract_countries_basic()
        
        # 2. TRANSFORM
        countries_transformed = transform_countries_basic(countries_raw)
        
        # 3. LOAD SA
        rows_inserted = load_to_sa(countries_transformed, execution_id)
        
        # Statistics
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
        print(f"Status: {stats['status']}")
        print(f"Execution ID: {stats['execution_id']}")
        print(f"Countries extracted: {stats['countries_extracted']}")
        print(f"Countries transformed: {stats['countries_transformed']}")
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


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    # Execute ETL
    execution_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    stats = etl_get_countries_basic(execution_id=execution_id)
    
    print("\n📊 Summary:")
    print(json.dumps(stats, indent=2))
