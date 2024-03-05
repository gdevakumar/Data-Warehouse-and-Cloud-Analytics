import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryDeleteDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


default_args = {
    'owner': 'deva',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='pollution_dataset_etl',
    default_args=default_args,
    description='Perform ETL Operations and load data to Data Warehouse',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2023, 5, 1)
)

PROJECT_ID='us-pollution-data'
GCS_PATH = 'dataset-tables/'
BUCKET_NAME = 'pollution-dataset'
LOCATION = 'us-west1'

start = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

create_staging_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id = 'create_staging_dataset',
    dag = dag,
    dataset_id = 'staging_dataset',
    project_id = 'us-pollution-data',
    location = LOCATION
)

create_staging_table_dates = BigQueryCreateEmptyTableOperator(
    task_id = 'create_table_dates',
    dag = dag,
    table_id = 'dates',
    dataset_id = 'staging_dataset',
    schema_fields = [
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'month', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'day', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'date_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]
)

create_staging_table_locations = BigQueryCreateEmptyTableOperator(
    task_id = 'create_table_locations',
    dag = dag,
    table_id = 'locations',
    dataset_id = 'staging_dataset',
    schema_fields = [
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'county', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'location_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]
)

create_staging_table_pollution = BigQueryCreateEmptyTableOperator(
    task_id = 'create_table_pollution',
    dag = dag,
    table_id = 'pollution',
    dataset_id = 'staging_dataset',
    schema_fields = [
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'county', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'o3_mean', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'o3_max_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'o3_max_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'o3_aqi', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'co_mean', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'co_max_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'co_max_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'co_aqi', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'so2_mean', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'so2_max_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'so2_max_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'so2_aqi', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'no2_mean', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'no2_max_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'no2_max_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'no2_aqi', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'date_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'location_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]
)

loading_dates = GCSToBigQueryOperator(
    task_id = 'loading_dates_table',
    dag = dag,
    bucket = BUCKET_NAME,
    source_objects = ['dataset-tables/dates_dim.csv'],
    destination_project_dataset_table = f'{PROJECT_ID}:staging_dataset.dates',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    allow_quoted_newlines = 'true',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'month', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'day', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'date_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]
)

loading_locations = GCSToBigQueryOperator(
    task_id = 'loading_locations',
    dag = dag,
    bucket = BUCKET_NAME,
    source_objects = ['dataset-tables/location_dim.csv'],
    destination_project_dataset_table = f'{PROJECT_ID}:staging_dataset.locations',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    allow_quoted_newlines = 'true',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'county', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'location_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]
)

loading_pollution = GCSToBigQueryOperator(
    task_id = 'loading_pollution',
    dag = dag,
    bucket = BUCKET_NAME,
    source_objects = ['dataset-tables/pollution_fact.csv'],
    destination_project_dataset_table = f'{PROJECT_ID}:staging_dataset.pollution',
    write_disposition='WRITE_TRUNCATE',
    source_format = 'csv',
    allow_quoted_newlines = 'true',
    skip_leading_rows = 1,
    schema_fields=[
        {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'county', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'o3_mean', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'o3_max_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'o3_max_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'o3_aqi', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'co_mean', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'co_max_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'co_max_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'co_aqi', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'so2_mean', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'so2_max_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'so2_max_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'so2_aqi', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'no2_mean', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'no2_max_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'no2_max_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'no2_aqi', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'date_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'location_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ]
)

check_dates = BigQueryCheckOperator(
        task_id = 'check_dates_table',
        dag = dag,
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.staging_dataset.dates`'
    )

check_locations = BigQueryCheckOperator(
        task_id = 'check_locations_table',
        dag = dag,
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.staging_dataset.locations`'
    )

check_pollution = BigQueryCheckOperator(
        task_id = 'check_pollution_table',
        dag = dag,
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.staging_dataset.pollution`'
    )

remove_nulls = BigQueryOperator(
    task_id = "remove_null_values",
    dag = dag,
    use_legacy_sql = False,
    location = LOCATION,
    sql = './sql/null_values_removal.sql'
)

add_quarter_and_day = BigQueryOperator(
    task_id = "add_quarter_and_day_column",
    dag = dag,
    use_legacy_sql = False,
    location = LOCATION,
    sql = './sql/add_cols.sql'
)

create_dw = BigQueryCreateEmptyDatasetOperator(
    task_id = 'create_data_warehouse',
    dag = dag,
    dataset_id = 'air_pollution',
    project_id = 'us-pollution-data',
    location = LOCATION
)

load_D_date = BigQueryOperator(
    task_id = "load_D_date_table",
    dag = dag,
    use_legacy_sql = False,
    location = LOCATION,
    sql = './sql/D_dates.sql'
)

load_D_location = BigQueryOperator(
    task_id = "load_D_location_table",
    dag = dag,
    use_legacy_sql = False,
    location = LOCATION,
    sql = './sql/D_locations.sql'
)

load_F_pollution = BigQueryOperator(
    task_id = "load_F_pollution_table",
    dag = dag,
    use_legacy_sql = False,
    location = LOCATION,
    sql = './sql/F_pollution.sql'
)

delete_staging_dataset = BigQueryDeleteDatasetOperator(
    task_id = 'cleanup_staging_dataset',
    project_id = PROJECT_ID,
    dataset_id = 'staging_dataset',
    dag = dag,
    delete_contents = True
)

end = DummyOperator(
    task_id='finish_pipeline',
    dag=dag
)

start >> create_staging_dataset >> [create_staging_table_dates, create_staging_table_locations, create_staging_table_pollution]

create_staging_table_dates >> loading_dates >> check_dates
create_staging_table_locations >> loading_locations >> check_locations
create_staging_table_pollution >> loading_pollution >> check_pollution

check_pollution >> remove_nulls 
check_dates >> add_quarter_and_day

[remove_nulls, add_quarter_and_day] >> create_dw >> [load_D_date, load_D_location, load_F_pollution]

[load_D_date, load_D_location, load_F_pollution] >> delete_staging_dataset >> end
