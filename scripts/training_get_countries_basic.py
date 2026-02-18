#!/usr/bin/env python3
"""
ETL: Get Countries - Campos BÁSICOS
====================================

**Objetivo Funcional:** Cargar campos básicos de todos los países

**Endpoints:**
- GET /all?fields=cca2,cca3,name,capital,region,subregion,area,population

**Flujo ETL:**
1. EXTRACT: Llamar API REST Countries (8 campos)
2. TRANSFORM: Normalizar nombres y estructuras
3. LOAD SA: TRUNCATE + INSERT en sa_training_countries_basic

**Tabla destino:** ga_integration.sa_training_countries_basic

Este es el PASO 1 de 4 para cargar todos los datos de países:
- Paso 1: Cargar campos básicos (este script)
- Paso 2: Cargar campos geográficos
- Paso 3: Cargar campos culturales
- Paso 4: MERGE de las 3 SA en TH
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import execute_batch

# Importar cliente REST Countries
sys.path.insert(0, '/opt/airflow/scripts')
from training_rest_countries_client import RestCountriesClient


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


# =============================================================================
# EXTRACT
# =============================================================================

def extract_countries_basic() -> List[Dict[str, Any]]:
    """
    EXTRACT: Obtiene campos básicos de todos los países de la API
    
    Returns:
        Lista de diccionarios con datos de países (campos básicos)
    """
    print("=" * 80)
    print("📥 EXTRACT: Obteniendo campos básicos de países...")
    print("=" * 80)
    
    client = RestCountriesClient()
    result = client.get_all_countries_basic()
    
    print(f"\n{result}")
    
    if not result.ok:
        raise Exception(f"Error en API: {result.status} - {result.text}")
    
    countries = result.json_obj
    print(f"✅ Se obtuvieron {len(countries)} países")
    print(f"   Campos esperados: cca2, cca3, name, capital, region, subregion, area, population")
    
    return countries


# =============================================================================
# TRANSFORM
# =============================================================================

def transform_country_basic(country: Dict[str, Any]) -> Dict[str, Any]:
    """
    TRANSFORM: Normaliza campos básicos de un país
    
    Args:
        country: Objeto JSON del país de la API
        
    Returns:
        Dict con datos normalizados para SA
    """
    return {
        # Identificadores
        'code_iso2': country.get('cca2'),
        'code_iso3': country.get('cca3'),
        
        # Nombres
        'name_common': country.get('name', {}).get('common'),
        'name_official': country.get('name', {}).get('official'),
        'name_native': json.dumps(country.get('name', {}).get('nativeName', {})),
        
        # Geografía básica
        'capital': json.dumps(country.get('capital', [])),
        'region': country.get('region'),
        'subregion': country.get('subregion'),
        'area': country.get('area'),
        
        # Población
        'population': country.get('population'),
    }


def transform_countries_basic(countries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    TRANSFORM: Normaliza todos los países
    
    Args:
        countries: Lista de países de la API
        
    Returns:
        Lista de países transformados
    """
    print("\n" + "=" * 80)
    print("🔄 TRANSFORM: Normalizando campos básicos...")
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
    
    print(f"✅ Se transformaron {len(transformed)} países")
    print(f"   Ejemplo: {transformed[0]['name_common']} ({transformed[0]['code_iso3']})")
    print(f"            Región: {transformed[0]['region']}, Población: {transformed[0]['population']:,}")
    
    return transformed


# =============================================================================
# LOAD SA
# =============================================================================

def load_to_sa(countries: List[Dict[str, Any]], execution_id: str) -> int:
    """
    LOAD SA: Carga datos en Staging Area (TRUNCATE + INSERT)
    
    Args:
        countries: Lista de países transformados
        execution_id: ID único de ejecución
        
    Returns:
        Número de filas insertadas
    """
    print("\n" + "=" * 80)
    print(f"📤 LOAD SA: Cargando en ga_integration.sa_training_countries_basic...")
    print("=" * 80)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # TRUNCATE: Limpiar tabla SA
        print(f"🗑️  TRUNCATE ga_integration.sa_training_countries_basic")
        cur.execute("TRUNCATE TABLE ga_integration.sa_training_countries_basic")
        
        # INSERT: Cargar datos nuevos
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
        
        # Añadir execution_id a cada registro
        for country in countries:
            country['execution_id'] = execution_id
        
        print(f"📥 Insertando {len(countries)} países...")
        execute_batch(cur, insert_sql, countries, page_size=100)
        
        conn.commit()
        
        # Verificar
        cur.execute("SELECT COUNT(*) FROM ga_integration.sa_training_countries_basic")
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


# =============================================================================
# ORQUESTACIÓN ETL
# =============================================================================

def etl_get_countries_basic(execution_id: str = None) -> Dict[str, Any]:
    """
    ETL Completo: Get Countries Basic
    
    Args:
        execution_id: ID de ejecución (opcional, se genera si no se provee)
        
    Returns:
        Dict con estadísticas de la ejecución
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
        
        # Estadísticas
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
        print(f"Estado: {stats['status']}")
        print(f"Execution ID: {stats['execution_id']}")
        print(f"Países extraídos: {stats['countries_extracted']}")
        print(f"Países transformados: {stats['countries_transformed']}")
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


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    # Ejecutar ETL
    execution_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    stats = etl_get_countries_basic(execution_id=execution_id)
    
    print("\n📊 Resumen:")
    print(json.dumps(stats, indent=2))
