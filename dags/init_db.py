from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

tables = ['nornickel.market_data', 'nornickel.v_global_basket', 'nornickel.sync_state']  # Список твоих таблиц

with DAG(
        'init_db',
        default_args={'owner': 'airflow'},
        schedule_interval=None,
        start_date=datetime(2026,3,1),
        catchup=False,
        template_searchpath=['/opt/airflow/sql']
) as dag:
    for table_name in tables:
        SQLExecuteQueryOperator(
            task_id=f'create_{table_name}',
            conn_id='postgres_default',
            sql=f'create_{table_name}.sql'
        )