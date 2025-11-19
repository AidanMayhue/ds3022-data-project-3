import duckdb
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# ---------------------------------------------
# Setup
# ---------------------------------------------
nltk.download("vader_lexicon")

# Connect to DuckDB
conn = duckdb.connect("bluesky.db")  # your DuckDB file

# Ensure the table exists
tables = conn.execute("SHOW TABLES").fetchall()
if ("bluesky_events",) not in tables:
    raise ValueError("Table 'bluesky_events' not found in DuckDB.")

# ---------------------------------------------
# Load text column into Pandas
# ---------------------------------------------
print("Loading posts from DuckDB...")
df = conn.execute("SELECT rev, record->>'text' AS text FROM bluesky_events").df()

# Ensure text column exists
if "text" not in df.columns:
    raise KeyError("No 'text' column found in DuckDB 'record' JSON.")

# ---------------------------------------------
# Sentiment analysis
# ---------------------------------------------
print("Running sentiment analysis...")
sia = SentimentIntensityAnalyzer()

# Compute compound sentiment
df["sentiment_compound"] = df["text"].astype(str).apply(
    lambda txt: sia.polarity_scores(txt)["compound"]
)

# Label sentiment
def label_sentiment(c):
    if c > 0.05:
        return "positive"
    elif c < -0.05:
        return "negative"
    else:
        return "neutral"

df["sentiment_label"] = df["sentiment_compound"].apply(label_sentiment)

# ---------------------------------------------
# Write sentiment back to DuckDB
# ---------------------------------------------
print("Updating DuckDB table with sentiment scores...")
# Create new table with sentiment or add columns to existing table
conn.execute("""
ALTER TABLE bluesky_events 
ADD COLUMN IF NOT EXISTS sentiment_compound DOUBLE
""")
conn.execute("""
ALTER TABLE bluesky_events 
ADD COLUMN IF NOT EXISTS sentiment_label VARCHAR
""")

# Update rows individually using rev as key
for _, row in df.iterrows():
    conn.execute(
        """
        UPDATE bluesky_events
        SET sentiment_compound = ?, sentiment_label = ?
        WHERE rev = ?
        """,
        [row["sentiment_compound"], row["sentiment_label"], row["rev"]]
    )

print("Sentiment analysis complete! Results are now in DuckDB.")
conn.close()
