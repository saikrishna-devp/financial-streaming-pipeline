import json
import sqlite3
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import yaml


def load_config():
    with open('config/config.yaml') as f:
        return yaml.safe_load(f)


def get_db_engine(config):
    db = config['database']
    return create_engine(
        f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )


def create_stock_table(engine):
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
    print('Stock quotes table ready')


def process_batch(messages, engine, db_file):
    if not messages:
        return {}
    df = pd.DataFrame(messages)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['processed_at'] = datetime.utcnow()
    df['change_pct'] = pd.to_numeric(df['change_pct'], errors='coerce')

    try:
        df.to_sql('stock_quotes', engine, if_exists='append', index=False, method='multi')
        print(f'  Loaded {len(df)} quotes to PostgreSQL')
    except Exception as e:
        print(f'  PostgreSQL error: {e}')

    conn = sqlite3.connect(db_file)
    df.to_sql('stock_quotes', conn, if_exists='append', index=False)
    conn.close()

    return {
        'total_quotes': len(df),
        'symbols': df['symbol'].unique().tolist(),
        'avg_price': round(df['price'].mean(), 2),
        'flagged': int(df['is_flagged'].sum()),
    }


def run_consumer(duration_seconds=3600):
    config = load_config()
    engine = get_db_engine(config)
    db_file = config['pipeline']['db_file']
    topic = config['kafka']['topic']

    create_stock_table(engine)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        group_id=config['kafka']['group_id'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
    )

    print(f'Consumer started - listening to topic: {topic}')
    start_time = time.time()
    total_processed = 0
    batch = []

    while time.time() - start_time < duration_seconds:
        try:
            for message in consumer:
                batch.append(message.value)
                if time.time() - start_time >= duration_seconds:
                    break
        except Exception:
            pass

        if batch:
            summary = process_batch(batch, engine, db_file)
            total_processed += len(batch)
            print(f'  Processed {len(batch)} quotes | Total: {total_processed} | Symbols: {summary.get("symbols",[])}')
            batch = []

    consumer.close()
    print(f'Consumer complete - {total_processed} quotes processed')
    return total_processed
