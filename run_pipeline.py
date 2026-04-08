import os
import time
import threading
from datetime import datetime
import yaml


def load_config():
    with open('config/config.yaml') as f:
        return yaml.safe_load(f)


def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{ts} | {msg}')


def setup_database(config):
    log('Setting up database tables...')
    from sqlalchemy import create_engine, text
    db = config['database']
    engine = create_engine(
        f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )
    sql = """
    CREATE TABLE IF NOT EXISTS stock_quotes (
        transaction_id  VARCHAR(50) PRIMARY KEY,
        symbol          VARCHAR(10),
        price           DECIMAL(10,2),
        open            DECIMAL(10,2),
        high            DECIMAL(10,2),
        low             DECIMAL(10,2),
        volume          BIGINT,
        change          DECIMAL(10,2),
        change_pct      VARCHAR(10),
        previous_close  DECIMAL(10,2),
        timestamp       TIMESTAMP,
        is_flagged      BOOLEAN,
        processed_at    TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    log('Database tables created successfully')
    return engine


def wait_for_kafka(bootstrap_servers, retries=10):
    from kafka import KafkaProducer
    log('Waiting for Kafka to be ready...')
    for i in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            log('Kafka is ready!')
            return True
        except Exception:
            log(f'  Kafka not ready - retry {i+1}/{retries}')
            time.sleep(3)
    raise Exception('Kafka did not become ready in time')


def run_producer_thread(config):
    from src.producer.transaction_producer import run_producer
    run_producer(duration_seconds=3600, tps=1)


def run_consumer_thread(config):
    from src.consumer.stream_processor import run_consumer
    run_consumer(duration_seconds=3600)


def show_results(config):
    import sqlite3
    import pandas as pd
    log('=' * 60)
    log('CURRENT STOCK DATA')
    log('=' * 60)

    try:
        conn = sqlite3.connect(config['pipeline']['db_file'])
        queries = {
            'Latest stock quotes': """
                SELECT symbol, price, change, change_pct, volume, timestamp
                FROM stock_quotes
                ORDER BY timestamp DESC
                LIMIT 10
            """,
            'Price summary by symbol': """
                SELECT symbol,
                       COUNT(*) as quotes,
                       ROUND(AVG(price),2) as avg_price,
                       ROUND(MIN(price),2) as min_price,
                       ROUND(MAX(price),2) as max_price
                FROM stock_quotes
                GROUP BY symbol
                ORDER BY symbol
            """,
        }
        for title, sql in queries.items():
            print(f'\n  {title}')
            print('  ' + '-'*50)
            try:
                result = pd.read_sql_query(sql, conn)
                print(result.to_string(index=False))
            except Exception as e:
                print(f'  No data yet: {e}')
        conn.close()
    except Exception as e:
        log(f'Results error: {e}')


if __name__ == '__main__':
    config = load_config()
    os.makedirs('data', exist_ok=True)

    log('=' * 60)
    log('  REAL-TIME STOCK MARKET STREAMING PIPELINE')
    log('  Alpha Vantage API + Kafka + PostgreSQL + Dashboard')
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f'  {ts}')
    log('=' * 60)

    setup_database(config)
    wait_for_kafka(config['kafka']['bootstrap_servers'])

    log('Starting producer and consumer...')
    log('Producer fetches real stock quotes every 60 seconds')
    log('Dashboard updates every 30 seconds')
    log('-' * 60)

    producer_thread = threading.Thread(target=run_producer_thread, args=(config,), name='ProducerThread')
    consumer_thread = threading.Thread(target=run_consumer_thread, args=(config,), name='ConsumerThread')

    producer_thread.start()
    time.sleep(5)
    consumer_thread.start()

    log('Both threads running!')
    log('Opening dashboard in 10 seconds...')
    time.sleep(10)

    show_results(config)

    log('Launching dashboard at http://localhost:8050')
    log('Press Ctrl+C to stop')

    from src.dashboard.dashboard import create_app
    app = create_app(config['pipeline']['db_file'])
    app.run(debug=False, port=8050)
