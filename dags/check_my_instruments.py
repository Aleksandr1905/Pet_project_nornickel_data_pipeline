import os, logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from t_tech.invest import Client
from t_tech.invest.utils import now

logger = logging.getLogger("airflow.task")


def check_instruments():
    token = os.getenv('T_INVEST_TOKEN')
    with Client(token) as client:
        # 1. Выводим ВЕСЬ список валют и металлов (они там)
        currencies = client.instruments.currencies().instruments
        logger.info("--- СПИСОК ВСЕХ ДОСТУПНЫХ ВАЛЮТ И МЕТАЛЛОВ ---")
        for i in currencies:
            # Выводим тикер и имя, чтобы мы узнали Палладий
            logger.info(f"Ticker: {i.ticker} | Name: {i.name} | FIGI: {i.figi} | UID: {i.uid}")


with DAG('check_instruments', start_date=now(), schedule_interval=None) as dag:
    PythonOperator(task_id='check', python_callable=check_instruments)
