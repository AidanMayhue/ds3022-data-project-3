import duckdb
import json
import numpy as np
import logging
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    conn = duckdb.connect("events.duckdb")
except Exception as e:
    logging.error(f"Could not connect to DuckDB: {e}")
    raise

try:
    rows = conn.execute("SELECT event_json FROM events").fetchall()
except Exception as e:
    logging.error(f"Failed to fetch events: {e}")
    rows = []

texts = []
for (event_json,) in rows:
    try:
        if isinstance(event_json, dict):
            texts.append(event_json.get("text", ""))
    except Exception as e:
        logging.warning(f"Skipping malformed record: {e}")

if not texts:
    logging.warning("No valid text found. Exiting.")
    exit()

try:
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(texts, show_progress_bar=True)
    np.save("bluesky_embeddings.npy", embeddings)
    logging.info(f"Generated embeddings for {len(texts)} posts.")
except Exception as e:
    logging.error(f"Failed to generate embeddings: {e}")
