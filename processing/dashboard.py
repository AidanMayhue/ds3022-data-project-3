import duckdb
import pandas as pd
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from wordcloud import WordCloud
import base64
from io import BytesIO

# Helper: wrap long hover text
def wrap_text(s, width=60):
    import textwrap
    if not isinstance(s, str):
        return ""
    return "<br>".join(textwrap.wrap(s, width))

# Helper: generate a word cloud and return it as base64 PNG
def generate_wordcloud(text_series, max_words=20):
    all_text = " ".join([t for t in text_series.dropna().astype(str)])

    wc = WordCloud(
        width=800,
        height=400,
        max_words=max_words,
        background_color="white",
        colormap="viridis"
    ).generate(all_text)

    buffer = BytesIO()
    wc.to_image().save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode()
    return f"data:image/png;base64,{encoded}"

# Load data from DuckDB
conn = duckdb.connect("events.duckdb", read_only=True)
df = conn.execute("""
    SELECT
      kafka_key,
      event_json->>'$.commit.record.text' AS text,
      sentiment_label,
      sentiment_compound
    FROM events
    WHERE event_json->>'$.commit.record.text' IS NOT NULL
""").fetchdf()
conn.close()

# Remove rows missing sentiment scores
df = df.dropna(subset=['sentiment_compound'])

# Standardize label casing
df['sentiment_label'] = df['sentiment_label'].str.lower()

# Colors for sentiment
color_map = {
    'positive': 'green',
    'negative': 'red',
    'neutral': 'gray'
}

# Dash app layout
app = Dash(__name__)

app.layout = html.Div([
    html.H1("Bluesky Posts Sentiment Dashboard"),

    dcc.Graph(id='sentiment-distribution'),
    dcc.Graph(id='sentiment-label-counts'),
    dcc.Graph(id='most-negative'),
    dcc.Graph(id='most-positive'),

    html.H2("Top 20 Most Frequent Words"),
    html.Img(id="wordcloud-img", style={"width": "80%", "height": "auto"})
])

# Main callback to generate all charts
@app.callback(
    Output('sentiment-distribution', 'figure'),
    Output('sentiment-label-counts', 'figure'),
    Output('most-negative', 'figure'),
    Output('most-positive', 'figure'),
    Output('wordcloud-img', 'src'),
    Input('sentiment-distribution', 'id')  # dummy trigger on load
)
def update_graphs(_):

    # Sentiment histogram
    fig_dist = px.histogram(
        df,
        x='sentiment_compound',
        nbins=50,
        color='sentiment_label',
        color_discrete_map=color_map,
        title="Sentiment Score Distribution"
    )

    # Counts per sentiment label
    label_counts = df['sentiment_label'].value_counts().reset_index()
    label_counts.columns = ['sentiment_label', 'count']

    fig_labels = px.bar(
        label_counts,
        x='sentiment_label',
        y='count',
        color='sentiment_label',
        color_discrete_map=color_map,
        title="Count by Sentiment Label"
    )

    # Top 10 negative posts
    most_negative = df.sort_values('sentiment_compound', ascending=True).head(10)
    most_negative = most_negative.reset_index(drop=True)
    most_negative['rank'] = most_negative.index + 1
    most_negative['wrapped_text'] = most_negative['text'].apply(lambda x: wrap_text(x, 60))

    fig_neg = px.bar(
        most_negative,
        x='rank',
        y='sentiment_compound',
        color='sentiment_label',
        color_discrete_map=color_map,
        title="Top 10 Most Negative Posts"
    )
    fig_neg.update_traces(customdata=most_negative['wrapped_text'])
    fig_neg.update_traces(
        hovertemplate="<b>Sentiment score:</b> %{y}<br><br><b>Post:</b><br>%{customdata}<extra></extra>"
    )

    # Top 10 positive posts
    most_positive = df.sort_values('sentiment_compound', ascending=False).head(10)
    most_positive = most_positive.reset_index(drop=True)
    most_positive['rank'] = most_positive.index + 1
    most_positive['wrapped_text'] = most_positive['text'].apply(lambda x: wrap_text(x, 60))

    fig_pos = px.bar(
        most_positive,
        x='rank',
        y='sentiment_compound',
        color='sentiment_label',
        color_discrete_map=color_map,
        title="Top 10 Most Positive Posts"
    )
    fig_pos.update_traces(customdata=most_positive['wrapped_text'])
    fig_pos.update_traces(
        hovertemplate="<b>Sentiment score:</b> %{y}<br><br><b>Post:</b><br>%{customdata}<extra></extra>"
    )

    # Word cloud (top 20 most frequent words)
    wordcloud_img = generate_wordcloud(df['text'], max_words=20)

    return fig_dist, fig_labels, fig_neg, fig_pos, wordcloud_img

# Start the server
if __name__ == '__main__':
    app.run(debug=True)
