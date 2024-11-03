from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    'bq_average_calculator',
    default_args=default_args,
    description='Calculate averages from BigQuery data',
    schedule_interval='@daily'
)

# SQL to calculate averages by product_type
calculate_averages = """
CREATE OR REPLACE TABLE `our-bebop-431708-i3.bcn.product_averages` AS
SELECT 
    product_type,
    AVG(value) as average_value,
    COUNT(*) as count,
    MAX(time_ref) as last_updated
FROM `our-bebop-431708-i3.bcn.revised_raw`
GROUP BY product_type
"""

calculate_task = BigQueryExecuteQueryOperator(
    task_id='calculate_averages',
    sql=calculate_averages,
    use_legacy_sql=False,
    dag=dag
)

calculate_task