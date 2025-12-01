import duckdb
import json
import logging
import numpy as np
import os
from datetime import datetime, timezone
from sentence_transformers import SentenceTransformer

# Configure basic logging format and level
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# Path to the DuckDB database storing Kafka events
DB_PATH = "events.duckdb"

def load_texts_and_times_from_db():
    try:
        # Connect to the DuckDB database file
        conn = duckdb.connect(DB_PATH)
    except Exception as e:
        logging.error(f"Unable to connect to DuckDB at {DB_PATH}: {e}")
        raise

    try:
        # Fetch Kafka keys and JSON payloads from stored events
        rows = conn.execute("SELECT kafka_key, event_json FROM events").fetchall()
    except Exception as e:
        logging.error(f"Failed to SELECT from events table: {e}")
        return [], [], []

    texts = []  # extracted text content from posts
    keys = []   # Kafka message keys
    times = []  # converted timestamps

    for kafka_key, raw in rows:
        try:
            # Parse JSON if necessary (string → dict)
            obj = raw if isinstance(raw, dict) else json.loads(raw)
        except Exception as e:
            logging.warning(f"Skipping row with key={kafka_key}: invalid JSON: {e}")
            continue

        # Extract time_us (if present) — microseconds since Unix epoch
        time_us = obj.get("time_us")
        if time_us is None:
            event_time = None
        else:
            try:
                # Convert microseconds → seconds → datetime in UTC
                ts = int(time_us) / 1_000_000
                event_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception as e:
                logging.warning(f"Invalid time_us for key={kafka_key}: {time_us}, error: {e}")
                event_time = None

        # Navigate to the nested commit.record.text field containing the actual post
        commit = obj.get("commit")
        if not commit:
            continue
        record = commit.get("record")
        if not record or not isinstance(record, dict):
            continue
        text = record.get("text")
        if not text:
            continue

        # Append successfully extracted fields
        keys.append(kafka_key)
        texts.append(text)
        times.append(event_time)

    logging.info(f"Loaded {len(texts)} text records from DuckDB for embedding (with time_us where available).")
    return keys, texts, times

def embed_texts(texts, batch_size=32):
    try:
        # Load the sentence-transformer model for embedding text
        model = SentenceTransformer("all-MiniLM-L6-v2")
    except Exception as e:
        logging.error(f"Could not load embedding model: {e}")
        raise

    try:
        # Encode text into vector embeddings using batching for speed
        embeddings = model.encode(texts,
                                  show_progress_bar=True,
                                  batch_size=batch_size,
                                  convert_to_numpy=True,
                                  normalize_embeddings=True)
        logging.info(f"Computed embeddings for {len(texts)} texts")
        return embeddings
    except Exception as e:
        logging.error(f"Error while embedding texts: {e}")
        raise

def save_embeddings_meta(embeddings, keys, times,
                         out_path="bluesky_embeddings.npz"):

    # If the output file already exists, delete it to avoid mixing old and new results
    if os.path.exists(out_path):
        os.remove(out_path)

    try:
        # Convert datetime objects to ISO-formatted strings for storage
        times_str = [t.isoformat() if t is not None else None for t in times]

        # Save embeddings + keys + timestamps into a single NPZ archive
        np.savez(out_path,
                 embeddings=embeddings,
                 keys=np.array(keys, dtype=object),
                 timestamps=np.array(times_str, dtype=object))
        logging.info(f"Saved embeddings + keys + timestamps to {out_path}")
    except Exception as e:
        logging.error(f"Failed to save embeddings/meta: {e}")
        raise

def main():
    # Load all text + metadata from DuckDB
    keys, texts, times = load_texts_and_times_from_db()
    if not texts:
        logging.warning("No texts found to embed — exiting.")
        return

    # Compute vector embeddings for all text content
    embeddings = embed_texts(texts)

    # Save embeddings along with keys + timestamps
    save_embeddings_meta(embeddings, keys, times)

# Standard Python entry point
if __name__ == "__main__":
    main()
