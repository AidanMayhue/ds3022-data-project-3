from quixstreams import Application
import json
import duckdb
import time

KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
TOPIC_NAME = "bluesky-events"
DUCKDB_FILE = "events.duckdb"

def main():
    # Connect to DuckDB (creates file if it doesn't exist)
    conn = duckdb.connect(DUCKDB_FILE)

    # Create table if it doesn't already exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            kafka_key TEXT,
            kafka_offset BIGINT,
            event_json JSON
        )
    """)

    app = Application(
        broker_address=KAFKA_BROKER,
        loglevel="DEBUG",
        consumer_group="bluesky-consumer",
        auto_offset_reset="earliest",
        producer_extra_config={
            "broker.address.family": "v4"
        }
    )

    while True:
        try:
            with app.get_consumer() as consumer:
                consumer.subscribe([TOPIC_NAME])
                print("Consumer started. Listening for messages...")

                while True:
                    msg = consumer.poll(1)

                    if msg is None:
                        continue

                    if msg.error() is not None:
                        print(f"Consumer error: {msg.error()}")
                        continue

                    # Parse and store message safely
                    try:
                        key = msg.key().decode("utf8") if msg.key() else None
                        value = json.loads(msg.value())
                        offset = msg.offset()

                        conn.execute(
                            """
                            INSERT INTO events (kafka_key, kafka_offset, event_json)
                            VALUES (?, ?, ?)
                            """,
                            (key, offset, json.dumps(value))
                        )

                        # Commit Kafka offset
                        consumer.store_offsets(msg)

                        print(f"Stored offset={offset}, key={key}")

                    except json.JSONDecodeError as e:
                        print(f"JSON decode error for message at offset {msg.offset()}: {e}")
                    except Exception as e:
                        print(f"Error processing message at offset {msg.offset()}: {e}")

        except Exception as e:
            print(f"Consumer connection error: {e}, reconnecting in 5 seconds...")
            time.sleep(5)  # Backoff before retrying

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
