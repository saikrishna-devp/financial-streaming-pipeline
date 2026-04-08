import json
import sqlite3
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import yaml


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def get_db_engine(config):
    db = config["database"]
    return create_engine(
        "postgresql+psycopg2://" + db["user"] + ":" + db["password"] + "@" + db["host"] + ":" + str(db["port"]) + "/" + db["name"]
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
    print("Stock quotes table ready")


def validate_batch(df):
    print("  Running data quality checks...")
    checks = []

    def check(name, passed, detail=""):
        status = "PASS" if passed else "FAIL"
        print("    [" + status + "] " + name + " " + detail)
        checks.append(passed)

    check("No null symbols",
          df["symbol"].notna().all(),
          "(" + str(df["symbol"].isna().sum()) + " nulls)")

    check("No null transaction_ids",
          df["transaction_id"].notna().all(),
          "(" + str(df["transaction_id"].isna().sum()) + " nulls)")

    check("Price is positive",
          (pd.to_numeric(df["price"], errors="coerce") > 0).all(),
          "(min=" + str(round(pd.to_numeric(df["price"], errors="coerce").min(), 2)) + ")")

    check("Volume is positive",
          (pd.to_numeric(df["volume"], errors="coerce") >= 0).all(),
          "(min=" + str(pd.to_numeric(df["volume"], errors="coerce").min()) + ")")

    check("Symbol is valid stock",
          df["symbol"].isin(["AAPL","GOOGL","MSFT","AMZN","TSLA"]).all(),
          "(found: " + str(df["symbol"].unique().tolist()) + ")")

    check("Timestamp is parseable",
          pd.to_datetime(df["timestamp"], errors="coerce").notna().all(),
          "")

    passed = sum(checks)
    total  = len(checks)
    print("  Result: " + str(passed) + "/" + str(total) + " checks passed")
    return passed == total


def process_batch(messages, engine, db_file):
    if not messages:
        return {}

    df = pd.DataFrame(messages)
    df["timestamp"]    = pd.to_datetime(df["timestamp"])
    df["processed_at"] = datetime.utcnow()
    df["change_pct"]   = pd.to_numeric(df["change_pct"], errors="coerce")
    df["price"]        = pd.to_numeric(df["price"], errors="coerce")
    df["volume"]       = pd.to_numeric(df["volume"], errors="coerce")

    is_valid = validate_batch(df)

    if not is_valid:
        print("  WARNING: Some checks failed - filtering invalid records")
        df = df[df["price"] > 0]
        df = df[df["symbol"].isin(["AAPL","GOOGL","MSFT","AMZN","TSLA"])]
        df = df.dropna(subset=["transaction_id","symbol","price"])

    try:
        df.to_sql("stock_quotes", engine, if_exists="append", index=False, method="multi")
        print("  Loaded " + str(len(df)) + " quotes to PostgreSQL")
    except Exception as e:
        print("  PostgreSQL error: " + str(e))

    conn = sqlite3.connect(db_file)
    df.to_sql("stock_quotes", conn, if_exists="append", index=False)
    conn.close()

    return {
        "total_quotes": len(df),
        "symbols":      df["symbol"].unique().tolist(),
        "avg_price":    round(df["price"].mean(), 2),
        "flagged":      int(df["is_flagged"].sum()),
    }


def run_consumer(duration_seconds=3600):
    config  = load_config()
    engine  = get_db_engine(config)
    db_file = config["pipeline"]["db_file"]
    topic   = config["kafka"]["topic"]

    create_stock_table(engine)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        group_id=config["kafka"]["group_id"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )

    print("Consumer started - listening to topic: " + topic)
    start_time      = time.time()
    total_processed = 0
    batch           = []

    while time.time() - start_time < duration_seconds:
        try:
            for message in consumer:
                batch.append(message.value)
                if time.time() - start_time >= duration_seconds:
                    break
        except Exception:
            pass

        if batch:
            print("Processing batch of " + str(len(batch)) + " messages...")
            summary = process_batch(batch, engine, db_file)
            total_processed += len(batch)
            print("  Total processed: " + str(total_processed) + " | Symbols: " + str(summary.get("symbols",[])))
            batch = []

    consumer.close()
    print("Consumer complete - " + str(total_processed) + " quotes processed")
    return total_processed
