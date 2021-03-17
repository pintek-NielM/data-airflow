# PT. PINDUIT TEKNOLOGI INDONESIA (PINTEK)
# Created by Herpaniel Rumende mangeka
# 10 FEBRUARI 2021

from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args ={
    'owner': 'herpaniel',
    'depends_on_past': False,
    'email': ['herpaniel.mangeka@gmail.com', 'andrea.philo@pintek.id'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Siplah_Transactional_Data',
    default_args=default_args,
    description='Create Siplah Transactional Table',
    schedule_interval ='0 0 * * *',
    start_date=days_ago(2),
    tags=['PINTEK'],
)
tables_transactional_data = ["siplah_transactional_data"]
export_siplah_transactional_data = tables_transactional_data
tables = []       
for siplah in export_siplah_transactional_data:
    Partition_Task = BigQueryOperator(
        task_id='{}_to_bigquery'.format(siplah),
        sql = "SELECT *,TIMESTAMP(FORMAT_TIMESTAMP('%F %T', current_timestamp() , 'UTC+7')) as PARTITIONTIME FROM pintek-production.ProductionLayer.{}".format(siplah),
        write_disposition = "WRITE_APPEND",
        location = "US", 
        bigquery_conn_id = "bigquery_default",
        use_legacy_sql = False, 
        destination_dataset_table='pintek-production.Playground.{}'.format(siplah),
        time_partitioning= {'time_partitioning_type':'DAY','field':'PARTITIONTIME'},
        dag = dag
    )
    tables.append(Partition_Task)
Partition_Task
