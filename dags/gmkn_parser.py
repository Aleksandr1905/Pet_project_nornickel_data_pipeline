import os
import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from t_tech.invest import CandleInterval, Client
from t_tech.invest.utils import now
from psycopg2.extras import execute_values
import yfinance as yf

logger = logging.getLogger('airflow_task')

def get_last_price(cursor, ticker):
    # Достаем из базы последнюю цену этого тикера
    cursor.execute(
        "SELECT price FROM nornickel.market_data WHERE ticker = %s ORDER BY timestamp DESC LIMIT 1",
        (ticker,)
    )
    res = cursor.fetchone()
    return res[0] if res else 0


def backfill_history():
    token = os.getenv('T_INVEST_TOKEN')

    insert_query = """
            INSERT INTO nornickel.market_data (timestamp, ticker, price, volume, asset_type)
            VALUES %s
            ON CONFLICT (timestamp, ticker) DO NOTHING
        """

    assets = {
        'GMKN': {'figi': 'BBG004731489', 'type': 'stock'}
    }
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:

            with Client(token=token) as client:
                for ticker, info in assets.items():
                    logger.info(f'Начинаю сбор данных {ticker}')
                    batch = []

                    for candle in client.get_all_candles(
                            instrument_id=info['figi'],
                            from_=now() - timedelta(days=1095),
                            interval=CandleInterval.CANDLE_INTERVAL_HOUR
                    ):
                        batch.append((
                            candle.time,
                            ticker,
                            candle.close.units + candle.close.nano / 1_000_000_000,
                            candle.volume,
                            info['type']
                        ))

                        if len(batch) >= 5000:
                            execute_values(cursor, insert_query, batch)
                            conn.commit()
                            logger.info(f"Загружен батч 5000 для {ticker}")
                            batch = []
                    if batch:
                        execute_values(cursor, insert_query, batch)
                        conn.commit()
                        logger.info(f"Загружены остатки ({len(batch)} строк) для {ticker}")

    logger.info(" Backfill завершен успешно")


def fetch_lme_history():
    lme_assets = {
        'NICKEL': 'NICK.L',
        'COPPER': 'HG=F',
        'PALLADIUM': 'PA=F',
        'PLATINUM': 'PL=F',
        'USDRUB': 'RUB=X'
    }

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    insert_query = """
        INSERT INTO nornickel.market_data (timestamp, ticker, price, volume, asset_type)
        VALUES %s ON CONFLICT (timestamp, ticker) DO NOTHING
    """

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for ticker, yf_code in lme_assets.items():
                logger.info(f"Загружаю мировые котировки для {ticker} ({yf_code})...")

                df = yf.download(yf_code, period="2y", interval="1h")

                batch = []

                if df.empty:
                    logger.warning(f"Данные по {ticker} не найдены!")
                    continue

                for timestamp, row in df.iterrows():
                    batch.append((
                        timestamp,
                        ticker,
                        float(row['Close']),
                        int(row['Volume']) if 'Volume' in row else 0,
                        'metal_lme'
                    ))

                execute_values(cursor, insert_query, batch)
                conn.commit()
                logger.info(f"Загружено {len(batch)} строк для мирового {ticker}")


with DAG(
        'nornickel_history_sync',
        default_args={'owner': 'airflow'},
        schedule_interval=None,
        start_date=datetime(2026, 3, 1),
        catchup=False
) as dag:
    tbank_sync = PythonOperator(
        task_id='tbank_sync',
        python_callable=backfill_history
    )
    yahoo_sync = PythonOperator(
        task_id='yahoo_sync',
        python_callable=fetch_lme_history
    )
    [tbank_sync, yahoo_sync]