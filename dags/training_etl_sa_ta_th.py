#!/usr/bin/env python3
"""
Training DAG: ETL Architecture with SA-TA-TH Pattern
====================================================

Demonstrates ETL flow using public APIs:
- REST Countries API (countries data)
- AQICN API (air quality and weather)

Layers:
- SA (Staging Area): Landing zone with TRUNCATE+INSERT
- TA (Auxiliary Tables): Transformations (not used in this simple example)
- TH (Historical Tables): Persistence with MERGE or INSERT append-only
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')

# Import ETL functions
from training_get_countries_basic import etl_get_countries_basic
from training_get_countries_geo import etl_get_countries_geo
from training_get_countries_culture import etl_get_countries_culture
from training_merge_countries_to_th import etl_merge_countries_to_th
from training_get_regions_stats import etl_get_regions_stats
from training_get_weather import etl_get_weather_data
from training_get_air_quality_aqicn import etl_get_air_quality


# DAG Configuration
default_args = {
    'owner': 'training',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='training_etl_sa_ta_th',
    default_args=default_args,
    description='Training: ETL with SA-TA-TH architecture using REST Countries API',
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['training', 'etl', 'sa-ta-th', 'rest-countries'],
)


# Wrappers for Airflow
def wrapper_get_countries_basic(**context):
    execution_id = context['run_id']
    print(f"Execution ID: {execution_id}")
    
    stats = etl_get_countries_basic(execution_id=execution_id)
    context['task_instance'].xcom_push(key='countries_basic_stats', value=stats)
    
    print(f"Countries BASIC ETL completed: {stats['rows_inserted_sa']} countries")
    return stats


def wrapper_get_countries_geo(**context):
    execution_id = context['run_id']
    print(f"Execution ID: {execution_id}")
    
    stats = etl_get_countries_geo(execution_id=execution_id)
    context['task_instance'].xcom_push(key='countries_geo_stats', value=stats)
    
    print(f"Countries GEO ETL completed: {stats['rows_inserted_sa']} countries")
    return stats


def wrapper_get_countries_culture(**context):
    execution_id = context['run_id']
    print(f"Execution ID: {execution_id}")
    
    stats = etl_get_countries_culture(execution_id=execution_id)
    context['task_instance'].xcom_push(key='countries_culture_stats', value=stats)
    
    print(f"Countries CULTURE ETL completed: {stats['rows_inserted_sa']} countries")
    return stats


def wrapper_merge_countries_to_th(**context):
    execution_id = context['run_id']
    print(f"Execution ID: {execution_id}")
    
    stats = etl_merge_countries_to_th(execution_id=execution_id)
    context['task_instance'].xcom_push(key='countries_merged_stats', value=stats)
    
    print(f"MERGE completed: {stats['countries_inserted']} new, {stats['countries_updated']} updated")
    return stats


def wrapper_get_regions_stats(**context):
    execution_id = context['run_id']
    print(f"Execution ID: {execution_id}")
    
    stats = etl_get_regions_stats(execution_id=execution_id)
    
    if not stats['success']:
        raise Exception(f"ETL Get Regions Stats failed: {stats['errors']}")
    
    context['task_instance'].xcom_push(key='regions_stats', value=stats)
    
    print(f"ETL completed:")
    print(f"   Regions processed: {', '.join(stats['regions_processed'])}")
    print(f"   Countries extracted: {stats['countries_extracted']}")
    print(f"   SA loaded: {stats['sa_loaded']}")
    print(f"   TH inserted: {stats['th_inserted']}")
    print(f"   TH updated: {stats['th_updated']}")
    
    return stats


def wrapper_get_weather(**context):
    execution_id = context['run_id']
    print(f"Execution ID: {execution_id}")
    
    stats = etl_get_weather_data(execution_id=execution_id)
    
    if not stats['success']:
        raise Exception(f"ETL Get Weather Data failed: {stats['errors']}")
    
    context['task_instance'].xcom_push(key='weather_stats', value=stats)
    
    print(f"ETL completed:")
    print(f"   Cities processed: {', '.join(stats['countries_processed'])}")
    print(f"   Measurements extracted: {stats['measurements_extracted']}")
    print(f"   SA loaded: {stats['sa_loaded']}")
    print(f"   TH inserted: {stats['th_inserted']}")
    print(f"   Duplicates ignored: {stats['th_duplicates']}")
    print(f"   Total in TH: {stats['th_total']}")
    
    return stats


def wrapper_get_air_quality(**context):
    execution_id = context['run_id']
    print(f"Execution ID: {execution_id}")
    
    stats = etl_get_air_quality(execution_id=execution_id)
    
    if stats.get('status') != 'SUCCESS':
        raise Exception(f"ETL Get Air Quality failed: {stats.get('status', 'UNKNOWN')}")
    
    context['task_instance'].xcom_push(key='air_quality_stats', value=stats)
    
    print(f"ETL completed:")
    print(f"   Measurements extracted: {stats['measurements_extracted']}")
    print(f"   SA loaded: {stats['rows_in_sa']}")
    print(f"   TH inserted: {stats['th_inserted']}")
    print(f"   Duplicates ignored: {stats['th_duplicates']}")
    print(f"   Total in TH: {stats['th_total']}")
    
    return stats


# Task Definitions
task_get_countries_basic = PythonOperator(
    task_id='get_countries_basic',
    python_callable=wrapper_get_countries_basic,
    dag=dag,
)

task_get_countries_geo = PythonOperator(
    task_id='get_countries_geo',
    python_callable=wrapper_get_countries_geo,
    dag=dag,
)

task_get_countries_culture = PythonOperator(
    task_id='get_countries_culture',
    python_callable=wrapper_get_countries_culture,
    dag=dag,
)

task_merge_countries_to_th = PythonOperator(
    task_id='merge_countries_to_th',
    python_callable=wrapper_merge_countries_to_th,
    dag=dag,
)

task_get_regions_stats = PythonOperator(
    task_id='get_regions_stats',
    python_callable=wrapper_get_regions_stats,
    dag=dag,
)

task_get_weather = PythonOperator(
    task_id='get_weather',
    python_callable=wrapper_get_weather,
    dag=dag,
)

task_get_air_quality = PythonOperator(
    task_id='get_air_quality',
    python_callable=wrapper_get_air_quality,
    dag=dag,
)


# Task Dependencies
# Countries: 3 parallel SA loads -> merge into TH
[task_get_countries_basic, task_get_countries_geo, task_get_countries_culture] >> task_merge_countries_to_th

# After merge, run independent parallel tasks
task_merge_countries_to_th >> [task_get_regions_stats, task_get_weather, task_get_air_quality]


print("DAG training_etl_sa_ta_th loaded successfully")
