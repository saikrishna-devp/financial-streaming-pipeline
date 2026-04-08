import json
import time
import requests
import yaml
from datetime import datetime
from kafka import KafkaProducer


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def get_stock_quote(symbol, api_key):
    url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=" + symbol + "&apikey=" + api_key
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        quote = data.get("Global Quote", {})
        if not quote:
            print("  No data for " + symbol)
            return None
        return {
            "transaction_id": "STK" + symbol + str(int(time.time())),
            "symbol":         symbol,
            "price":          float(quote.get("05. price", 0)),
            "open":           float(quote.get("02. open", 0)),
            "high":           float(quote.get("03. high", 0)),
            "low":            float(quote.get("04. low", 0)),
            "volume":         int(quote.get("06. volume", 0)),
            "change":         float(quote.get("09. change", 0)),
            "change_pct":     quote.get("10. change percent", "0%").replace("%", ""),
            "previous_close": float(quote.get("08. previous close", 0)),
            "timestamp":      datetime.utcnow().isoformat(),
            "is_flagged":     abs(float(quote.get("09. change", 0))) > 5,
        }
    except Exception as e:
        print("  Error fetching " + symbol + ": " + str(e))
        return None


def run_producer(duration_seconds=3600, tps=1):
    config   = load_config()
    api_key  = config["alphavantage"]["api_key"]
    symbols  = config["alphavantage"]["symbols"]
    poll_interval = config["pipeline"]["poll_interval_seconds"]

    producer = KafkaProducer(
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    topic = config["kafka"]["topic"]

    print("Producer started - polling every " + str(poll_interval) + "s")
    print("-" * 50)

    start_time = time.time()
    total_sent = 0
    poll_count = 0

    while time.time() - start_time < duration_seconds:
        poll_count += 1
        print("Poll " + str(poll_count) + " - fetching real stock quotes...")

        for symbol in symbols:
            quote = get_stock_quote(symbol, api_key)
            if quote:
                producer.send(topic, value=quote)
                total_sent += 1
                print("  " + symbol + ": price=" + str(quote["price"]) + " change=" + str(quote["change"]))
            time.sleep(12)

        producer.flush()
        print("Sent " + str(total_sent) + " quotes - waiting " + str(poll_interval) + "s...")
        time.sleep(poll_interval)

    producer.close()
    print("Producer complete - " + str(total_sent) + " quotes sent")
    return total_sent
