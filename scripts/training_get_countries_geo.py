#!/usr/bin/env python3
"""
ETL: Get Countries - Campos GEOGRÁFICOS
========================================

**Objetivo Funcional:** Cargar campos geográficos de todos los países

**Endpoints:**
- GET /all?fields=cca2,cca3,latlng,landlocked,borders

**Flujo ETL:**
1. EXTRACT: Llamar API REST Countries (5 campos)
2. TRANSFORM: Normalizar coordenadas y fronteras
3. LOAD SA: TRUNCATE + INSERT en sa_training_countries_geo

**Tabla destino:** ga_integration.sa_training_countries_geo

Este es el PASO 2 de 4 para cargar todos los datos de países.
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
    """EXTRACT: Obtiene campos geográficos de todos los países"""
    print("=" * 80)
    print("📥 EXTRACT: Obteniendo campos geográficos de países...")
    print("=" * 80)
    
    client = RestCountriesClient()
    result = client.get_all_countries_geo()
    
    print(f"\n{result}")
    
    if not result.ok:
        raise Exception(f"Error en API: {result.status} - {result.text}")
    
    countries = result.json_obj
    print(f"✅ Se obtuvieron {len(countries)} países")
    print(f"   Campos esperados: cca2, cca3, latlng, landlocked, borders")
    
    return countries


def transform_country_geo(country: Dict[str, Any]) -> Dict[str, Any]:
    """TRANSFORM: Normaliza campos geográficos de un país"""
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
    """TRANSFORM: Normaliza todos los países"""
    print("\n" + "=" * 80)
    print("🔄 TRANSFORM: Normalizando campos geográficos...")
    print("=" * 80)
    
    transformed = []
    for country in countries:
        try:
            data = transform_country_geo(country)
            transformed.append(data)
        except Exception as e:
            code = country.get('cca3', 'UNKNOWN')
            print(f"⚠️  Error transformando {code}: {e}")
            continue
    
    print(f"✅ Se transformaron {len(transformed)} países")
    
    # Mostrar ejemplo de país sin salida al mar
    landlocked_example = next((c for c in transformed if c.get('landlocked')), None)
    if landlocked_example:
        print(f"   Ejemplo landlocked: {landlocked_example['code_iso3']}")
        print(f"                       Coordenadas: ({landlocked_example['latitude']}, {landlocked_example['longitude']})")
    
    return transformed


def load_to_sa(countries: List[Dict[str, Any]], execution_id: str) -> int:
    """LOAD SA: Carga datos en Staging Area (TRUNCATE + INSERT)"""
    print("\n" + "=" * 80)
    print(f"📤 LOAD SA: Cargando en ga_integration.sa_training_countries_geo...")
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
        
        print(f"📥 Insertando {len(countries)} países...")
        execute_batch(cur, insert_sql, countries, page_size=100)
        
        conn.commit()
        
        cur.execute("SELECT COUNT(*) FROM ga_integration.sa_training_countries_geo")
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


def etl_get_countries_geo(execution_id: str = None) -> Dict[str, Any]:
    """ETL Completo: Get Countries Geo"""
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
    stats = etl_get_countries_geo(execution_id=execution_id)
    print("\n📊 Resumen:")
    print(json.dumps(stats, indent=2))
