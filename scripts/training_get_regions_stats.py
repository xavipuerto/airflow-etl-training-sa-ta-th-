#!/usr/bin/env python3
"""
Functional Script: Get Countries by REGION + Statistics
===========================================================

Endpoint: GET /region/{region}
Objective: Extract countries from a specific region and calculate statistics

Available regions:
- africa
- americas
- asia
- europe
- oceania

Complete flow SA → TH:
1. EXTRACT: Call API /region/{region} for each region
2. LOAD SA: TRUNCATE + INSERT into sa_training_regions_stats
3. MERGE TH: UPSERT from SA to th_training_regions_stats

This script demonstrates:
- Multiple calls to the same API with different parameters
- Data aggregation (COUNT, SUM, AVG)
- Complex transformations
"""
import sys
import psycopg2
from typing import List, Dict, Any
from datetime import datetime

sys.path.insert(0, '/opt/airflow/scripts')
from training_rest_countries_client import RestCountriesClient, extract_country_data


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

SCHEMA = 'ga_integration'
TABLE_SA = 'sa_training_regions_stats'
TABLE_TH = 'th_training_regions_stats'

# Regions available in REST Countries API
REGIONS = ['africa', 'americas', 'asia', 'europe', 'oceania']


# =============================================================================
# MAIN FUNCTION: COMPLETE ETL
# =============================================================================

def etl_get_regions_stats(execution_id: str = None, regions: List[str] = None) -> Dict[str, Any]:
    """
    Complete ETL: GET /region/{region} → Aggregation → SA → TH
    
    Args:
        execution_id: Execution ID
        regions: List of regions to process (default: all)
        
    Returns:
        Dict with complete statistics
    """
    if not execution_id:
        execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if not regions:
        regions = REGIONS
    
    start_time = datetime.now()
    print(f"\n{'='*80}")
    print(f"📊 ETL: Statistics by Region")
    print(f"   Endpoint: GET /region/{{region}}")
    print(f"   Regions: {', '.join(regions)}")
    print(f"   Execution ID: {execution_id}")
    print(f"   Timestamp: {start_time}")
    print(f"{'='*80}\n")
    
    stats = {
        'execution_id': execution_id,
        'start_time': start_time,
        'endpoint': '/region/{region}',
        'regions_processed': [],
        
        # EXTRACT phase
        'api_calls': 0,
        'countries_extracted': 0,
        
        # TRANSFORM phase
        'regions_calculated': 0,
        
        # LOAD SA phase
        'sa_truncated': False,
        'sa_loaded': 0,
        
        # MERGE TH phase
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
        # PHASE 1: EXTRACT - Get countries by region
        # =====================================================================
        print("📥 PHASE 1: EXTRACT - Getting countries by region")
        print("-" * 80)
        
        client = RestCountriesClient()
        
        for region in regions:
            print(f"\n🌍 Processing region: {region.upper()}")
            result = client.get_countries_by_region(region)
            stats['api_calls'] += 1
            
            print(f"   {result}")
            
            if not result.ok:
                error_msg = f"API error for region {region}: {result.status}"
                stats['errors'].append(error_msg)
                print(f"   ⚠️  {error_msg}")
                continue
            
            countries = result.json_obj
            print(f"   ✅ Retrieved {len(countries)} countries from {region}")
            
            # ================================================================
            # PHASE 2: TRANSFORM - Calculate region statistics
            # ================================================================
            print(f"   📊 Calculating statistics...")
            
            region_stats = calculate_region_stats(region, countries, execution_id)
            region_stats_list.append(region_stats)
            
            stats['countries_extracted'] += len(countries)
            stats['regions_processed'].append(region)
            
            print(f"   ✅ Statistics calculated for {region}:")
            print(f"      Total countries: {region_stats['total_countries']}")
            print(f"      Total population: {region_stats['total_population']:,}")
            print(f"      Average population: {region_stats['avg_population']:,.0f}")
        
        stats['regions_calculated'] = len(region_stats_list)
        
        print(f"\n✅ Processed {stats['regions_calculated']} regions")
        print(f"✅ Total countries extracted: {stats['countries_extracted']}")
        
        # =====================================================================
        # PHASE 3: LOAD SA - Load statistics to Staging Area
        # =====================================================================
        print(f"\n💾 PHASE 3: LOAD SA - Loading statistics to Staging table")
        print("-" * 80)
        
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # TRUNCATE SA
        print(f"🗑️  Truncating ga_integration.sa_training_regions_stats...")
        cursor.execute("TRUNCATE TABLE ga_integration.sa_training_regions_stats")
        stats['sa_truncated'] = True
        
        # INSERT into SA
        if region_stats_list:
            print(f"📝 Inserting {len(region_stats_list)} region records into SA...")
            insert_stats_sa(cursor, region_stats_list)
            stats['sa_loaded'] = len(region_stats_list)
            print(f"✅ {stats['sa_loaded']} regions loaded into SA")
        
        conn.commit()
        
        # =====================================================================
        # PHASE 4: MERGE TH - Consolidate into Historical Table
        # =====================================================================
        print(f"\n🔄 PHASE 4: MERGE TH - Consolidating SA → TH")
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
        
        print(f"✅ MERGE completed:")
        print(f"   📥 Inserted: {stats['th_inserted']} regions")
        print(f"   🔄 Updated: {stats['th_updated']} regions")
        
        # Total in TH
        cursor.execute("SELECT COUNT(*) FROM ga_integration.th_training_regions_stats")
        stats['th_total'] = cursor.fetchone()[0]
        print(f"   📊 Total in TH: {stats['th_total']} regions")
        
        conn.commit()
        stats['success'] = True
        
    except Exception as e:
        error_msg = f"Critical error: {e}"
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
    # SUMMARY
    # =========================================================================
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    stats['end_time'] = end_time
    stats['duration_seconds'] = duration
    
    print(f"\n{'='*80}")
    print(f"📊 SUMMARY ETL: Regions Statistics")
    print(f"{'='*80}")
    print(f"   Execution ID: {execution_id}")
    print(f"   Status: {'✅ SUCCESS' if stats['success'] else '❌ FAILED'}")
    print(f"   Duration: {duration:.2f} seconds")
    print(f"\n   📥 EXTRACT:")
    print(f"      API Calls: {stats['api_calls']}")
    print(f"      Regions processed: {', '.join(stats['regions_processed'])}")
    print(f"      Countries extracted: {stats['countries_extracted']}")
    print(f"\n   📊 TRANSFORM:")
    print(f"      Regions calculated: {stats['regions_calculated']}")
    print(f"\n   💾 LOAD SA:")
    print(f"      Truncated: {'✅' if stats['sa_truncated'] else '❌'}")
    print(f"      Loaded: {stats['sa_loaded']}")
    print(f"\n   🔄 MERGE TH:")
    print(f"      Inserted: {stats['th_inserted']}")
    print(f"      Updated: {stats['th_updated']}")
    print(f"      Total in TH: {stats['th_total']}")
    
    if stats['errors']:
        print(f"\n   ⚠️  Errors: {len(stats['errors'])}")
        for err in stats['errors'][:5]:
            print(f"      - {err}")
    
    print(f"{'='*80}\n")
    
    return stats


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def calculate_region_stats(region: str, countries: List[Dict], execution_id: str) -> Dict[str, Any]:
    """
    Calculate aggregated statistics for a region
    
    This is an example of TRANSFORM in ETL:
    - Aggregations (COUNT, SUM, AVG)
    - Conditional filters
    - Derived calculations
    """
    total_countries = len(countries)
    total_population = 0
    total_area = 0
    landlocked_count = 0
    independent_count = 0
    un_member_count = 0
    
    for country in countries:
        # Population
        pop = country.get('population', 0)
        if pop:
            total_population += pop
        
        # Area
        area = country.get('area', 0)
        if area:
            total_area += area
        
        # Landlocked
        if country.get('landlocked', False):
            landlocked_count += 1
        
        # Independent
        if country.get('independent', False):
            independent_count += 1
        
        # UN Member
        if country.get('unMember', False):
            un_member_count += 1
    
    # Average
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
    """Insert statistics into SA"""
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
    print("Test script: Get Regions Stats (Complete ETL)")
    print("📊" * 40 + "\n")
    
    stats = etl_get_regions_stats()
    
    if stats['success']:
        print(f"\n✅ ETL executed successfully")
        print(f"\n💡 Useful queries:")
        print(f"   -- View statistics for all regions:")
        print(f"   SELECT * FROM ga_integration.th_training_regions_stats ORDER BY total_population DESC;")
        print(f"\n   -- Region with most countries:")
        print(f"   SELECT region, total_countries FROM ga_integration.th_training_regions_stats ORDER BY total_countries DESC LIMIT 1;")
        print(f"\n   -- Region with highest population:")
        print(f"   SELECT region, total_population FROM ga_integration.th_training_regions_stats ORDER BY total_population DESC LIMIT 1;")
        print(f"\n   -- Compare versions (if already run before):")
        print(f"   SELECT region, version, last_updated_at FROM ga_integration.th_training_regions_stats ORDER BY last_updated_at DESC;")
    else:
        print(f"\n❌ ETL failed. See errors above.")
        sys.exit(1)
