import os
import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from t_tech.invest import CandleInterval, Client

from psycopg2.extras import execute_values
import yfinance as yf
import pendulum
import pandas as pd

logger = logging.getLogger('airflow_task')

def get_last_sync_date(cursor, ticker):
    cursor.execute(
        "SELECT last_date FROM nornickel.sync_state WHERE ticker = %s",
        (ticker,)
    )
    result = cursor.fetchone()
    return pendulum.instance(result[0]) if result else None

def update_last_sync(cursor, ticker, last_date):
    cursor.execute("""
        INSERT INTO nornickel.sync_state (ticker, last_date)
        VALUES (%s, %s)
        ON CONFLICT (ticker) DO UPDATE 
        SET last_date = EXCLUDED.last_date,
            updated_at = CURRENT_TIMESTAMP
    """, (ticker, last_date))


def sync_all_market_data(data_interval_start, data_interval_end, **kwargs):
    token = os.getenv('T_INVEST_TOKEN')
    start_dt = pendulum.parse(data_interval_start)
    end_dt = pendulum.parse(data_interval_end)

    insert_query = """
        INSERT INTO nornickel.market_data (timestamp, ticker, price, volume, asset_type)
        VALUES %s 
        ON CONFLICT (timestamp, ticker) 
        DO UPDATE SET 
            price = EXCLUDED.price,
            volume = EXCLUDED.volume,
            asset_type = EXCLUDED.asset_type
    """

    assets = {
        'GMKN': {'figi': 'BBG004731489', 'type': 'stock'}
    }
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:

            with Client(token=token) as client:
                for ticker, info in assets.items():

                    last_gmkn_date = get_last_sync_date(cursor, ticker)

                    logger.info(f'Начинаю сбор данных {ticker}')

                    batch = []

                    for candle in client.get_all_candles(
                            instrument_id=info['figi'],
                            from_=start_dt,
                            to=end_dt,
                            interval=CandleInterval.CANDLE_INTERVAL_HOUR
                    ):

                        if last_gmkn_date and candle.time <= last_gmkn_date:
                            continue

                        batch.append((
                            candle.time,
                            ticker,
                            candle.close.units + candle.close.nano / 1_000_000_000,
                            candle.volume,
                            info['type']
                        ))

                    if batch:
                        execute_values(cursor, insert_query, batch)
                        update_last_sync(cursor, ticker, batch[-1][0])
                        conn.commit()
                        logger.info(f"Загружены ({len(batch)} строк) для {ticker}")
                    else:
                        logger.info(f"Нет новых данных для {ticker}")
            logger.info(" Update T-Api завершен успешно")

            lme_assets = {
                'NICKEL': 'NICK.L',
                'COPPER': 'HG=F',
                'PALLADIUM': 'PA=F',
                'PLATINUM': 'PL=F',
                'USDRUB': 'RUB=X'
            }

            for ticker, yf_code in lme_assets.items():
                logger.info(f"Загружаю мировые котировки для {ticker} ({yf_code})...")

                last_date = get_last_sync_date(cursor, ticker)

                df = yf.download(
                    tickers=yf_code,
                    period='2d',
                    interval="1h",
                    progress=False
                )

                if df.empty:
                    continue

                if last_date:
                    df = df[df.index > last_date]


                if df.empty:
                    logger.info(f"Нет новых данных для {ticker}")
                    continue

                batch = []

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                for timestamp, row in df.iterrows():
                    batch.append((
                        timestamp,
                        ticker,
                        float(row['Close']),
                        int(row.get('Volume', 0)),
                        'metal_lme'
                    ))

                execute_values(cursor, insert_query, batch)

                update_last_sync(cursor, ticker, batch[-1][0])

                conn.commit()
                logger.info(f"Успешно записано для {ticker}")

        logger.info("Синхронизация Yahoo завершена.")


with DAG(
        'nornickel_increment_sync',
        default_args={'owner': 'airflow'},
        schedule="*/30 * * * *",
        start_date=datetime(2026, 3, 10),
        max_active_runs=1,
        catchup=True
) as dag:
    inc_sync = PythonOperator(
        task_id='inc_sync',
        python_callable=sync_all_market_data,
        op_kwargs={
            'data_interval_start': '{{data_interval_start}}',
            'data_interval_end': '{{data_interval_end}}',
        }
    )
