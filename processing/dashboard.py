import duckdb
import pandas as pd
import re
import logging
import plotly.express as px
from dash import Dash, dcc, html
from dash.dependencies import Input, Output

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    conn = duckdb.connect("events.duckdb")
    df = conn.execute("SELECT event_json, sentiment_label FROM events").fetchdf()
except Exception as e:
    logging.error(f"Failed to load data: {e}")
    df = pd.DataFrame(columns=['event_json', 'sentiment_label'])

if df.empty:
    logging.warning("No data available for dashboard.")
else:
    # Extract fields safely
    df['text'] = df['event_json'].apply(lambda x: x.get('text','') if isinstance(x, dict) else '')
    df['timestamp'] = df['event_json'].apply(lambda x: x.get('timestamp') if isinstance(x, dict) else None)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['hashtags'] = df['text'].apply(lambda t: re.findall(r"#(\w+)", t))

app = Dash(__name__)
app.layout = html.Div([
    html.H1("Bluesky Dashboard"),
    dcc.DatePickerRange(
        id='date-picker',
        min_date_allowed=df['timestamp'].min() if not df.empty else None,
        max_date_allowed=df['timestamp'].max() if not df.empty else None,
        start_date=df['timestamp'].min() if not df.empty else None,
        end_date=df['timestamp'].max() if not df.empty else None
    ),
    dcc.Graph(id='sentiment-over-time'),
    dcc.Graph(id='top-hashtags')
])

@app.callback(
    Output('sentiment-over-time', 'figure'),
    Output('top-hashtags', 'figure'),
    Input('date-picker', 'start_date'),
    Input('date-picker', 'end_date')
)
def update_charts(start_date, end_date):
    if df.empty:
        return px.line(title="No data"), px.bar(title="No data")
    
    filtered = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
    
    try:
        sentiment_time = filtered.groupby([pd.Grouper(key='timestamp', freq='H'), 'sentiment_label']).size().reset_index(name='count')
        fig_sentiment = px.line(sentiment_time, x='timestamp', y='count', color='sentiment_label', title='Sentiment Over Time')
    except Exception as e:
        logging.warning(f"Failed to create sentiment chart: {e}")
        fig_sentiment = px.line(title="Error generating chart")
    
    try:
        hashtags_flat = [tag for tags in filtered['hashtags'] for tag in tags]
        top_tags = pd.Series(hashtags_flat).value_counts().nlargest(20).reset_index()
        top_tags.columns = ['hashtag', 'count']
        fig_hashtags = px.bar(top_tags, x='hashtag', y='count', title='Top 20 Hashtags')
    except Exception as e:
        logging.warning(f"Failed to create hashtags chart: {e}")
        fig_hashtags = px.bar(title="Error generating chart")
    
    return fig_sentiment, fig_hashtags

if __name__ == "__main__":
    app.run_server(debug=True)
