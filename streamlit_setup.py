import json
from pyhive import hive
import happybase
import pandas as pd
import streamlit as st
from datetime import timedelta
from hdfs import InsecureClient
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from datetime import datetime


def get_binance_marketdata():
    namenode = "http://localhost:50070/"
    hdfs_directory_path = '/user/big_d_analytics/binance_marketdata'
    client = InsecureClient(namenode)
    files = client.list(hdfs_directory_path)
    prices = []
    for file_name in files:
        file_path = f"{hdfs_directory_path}/{file_name}"
        with client.read(file_path) as reader:
            raw_data = reader.read().decode().strip()
            if not raw_data:
                continue
            try:
                for line in raw_data.split("\n"):
                    if line.strip():
                        json_obj = json.loads(line)
                        prices.append(json_obj)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from {file_name}: {e}")
                print(f"Problematic Line: {line}")
    return pd.DataFrame(prices, columns=['symbol', 'price', 'timestamp'])


def get_hive_data(query):
    conn = hive.Connection(host="localhost")
    return pd.read_sql(query, conn)


def get_hbase_reddit_data():
    conn = happybase.Connection(host="localhost", port=9090)
    table = conn.table("reddit_sentiment")
    data_posts = []
    data_comments = []
    for key, row in table.scan():
        curr_post = [key.decode()]
        for col, val in row.items():
            if col.decode() == 'post_info:comments':
                for comment in json.loads(val.decode()):
                    data_comments.append(list(comment.values()) + curr_post)
            else:
                curr_post.append(val.decode())
        data_posts.append(curr_post)
    df_posts = pd.DataFrame(data_posts, columns=['post_key', 'post_score', 'post_time', 'post_title',
                                                 'post_upvote', 'comments_sentiment', 'post_sentiment'])
    df_comments = pd.DataFrame(data_comments, columns=['comment_id', 'comment_body', 'comment_time',
                                                       'comment_score', 'post_key'])
    conn.close()
    return df_posts, df_comments


st.title("BigD Analytics - cryptocurrency, weather and sentiment analysis")

query = "SELECT * FROM model_results"

# Step 2: Check if data is already in session state
if "hive_data" not in st.session_state:
    with st.spinner("Fetching data from Hive..."):
        try:
            st.session_state.hive_data = get_hive_data(query)
        except Exception as e:
            st.error(f"Error while downloading Hive data: {e}")
            st.session_state.hive_data = None

# Step 3: Use the DataFrame for further operations
model_results = st.session_state.hive_data
model_results.columns = model_results.columns.str.replace('model_results.', '')

if "reddit_comments" not in st.session_state or "reddit_posts" not in st.session_state:
    with st.spinner("Fetching data from HBase..."):
        try:
            st.session_state.reddit_posts, st.session_state.reddit_comments = get_hbase_reddit_data()
        except Exception as e:
            st.session_state.reddit_posts, st.session_state.reddit_comments = None, None
            st.error(f"Error while downloading HBase data: {e}")
df_posts, df_comments = st.session_state.reddit_posts, st.session_state.reddit_comments

# ---------------------------- Change hive dataframe -----------------------
# For printing on plots
model_results['date'] = pd.to_datetime(model_results['time_stamp'], unit='s')

# Sort (not necessary, but easier to look at)
model_results = model_results.sort_values('time_stamp').reset_index(drop=True)

# ---------------------------- Time slider for everything -----------------------
st.markdown('## Select time period')

# Slider to filter data by date
start_date = datetime.fromtimestamp(model_results['time_stamp'].min())
end_date = datetime.fromtimestamp(model_results['time_stamp'].max())

# Define the slider
start, end = st.slider(
    '',
    min_value=start_date,
    max_value=end_date,
    value=(start_date, end_date),
    format="YYYY/MM/DD - hh:mm",
    step=timedelta(minutes=1)
)

# ---------------------------- Binance market data -----------------------

st.header("Binance Market Data")


def plot_marketdata(df):
    figure = go.Figure()
    figure.add_trace(go.Scatter(x=df['date'], y=df['price'], mode='lines', name='Price'))
    figure.update_layout(
        title='Binance Market Data',
        xaxis_title='Time',
        yaxis_title='Price (USD)',
        xaxis_tickangle=-90,
        template="plotly_dark",
        xaxis=dict(
            tickformat='%Y/%m/%d %H:%M',  # Format the date as needed
        ),
    )
    st.plotly_chart(figure)


if "hdfs_data" not in st.session_state:
    with st.spinner("Fetching data from Hive..."):
        try:
            st.session_state.hdfs_data = get_binance_marketdata()
        except Exception as e:
            st.session_state.hdfs_data = None
            st.error(f"Error while downloading HDFS data: {e}")

df_prices = st.session_state.hdfs_data
df_prices = df_prices.sort_values('timestamp')
df_prices['date'] = pd.to_datetime(df_prices['timestamp'])
df_prices_filtered = df_prices[(df_prices['date'] >= start) & (df_prices['date'] <= end)]

plot_marketdata(df_prices_filtered)

# ---------------------------- Binance values and predictions -----------------------

st.markdown('## Model predictions')

filtered_results = model_results[(model_results['time_stamp'] >= datetime.timestamp(start)) & (
            model_results['time_stamp'] <= datetime.timestamp(end))]


def plot_binance_predictions(df):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['y'],
        mode='lines',
        name='True',
        line=dict(color='blue', width=2)
    ))

    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['y_hat'],
        mode='lines',
        name='Prediction',
        line=dict(color='red', dash='dash', width=2)
    ))

    fig.update_layout(
        title='Binance Prediction',
        xaxis_title='Date',
        yaxis_title='Bitcoin price change [USD]',
        xaxis_tickangle=-90,
        template='plotly_dark',  # Optional: choose a template (light or dark)
        xaxis=dict(
            tickformat='%Y/%m/%d %H:%M'  # Format the date as needed
        ),
        legend=dict(x=0, y=1, traceorder='normal', orientation='h')
    )
    st.plotly_chart(fig)


def plot_mse(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['mse'],
        mode='lines',
        name='Model MSE',
        line=dict(color='blue'),
        marker=dict(size=6, color='blue', opacity=0.6)
    ))

    fig.update_layout(
        title='Model MSE Over Time',
        xaxis_title='Date',
        yaxis_title='Model MSE',
        xaxis_tickangle=-90,
        template='plotly_dark',  # Optional: choose a template (light or dark)
        xaxis=dict(
            tickformat='%Y/%m/%d %H:%M',  # Format the date as needed
        ),
        yaxis=dict(
            zeroline=True,
            showgrid=True
        ),
        height=600
    )
    st.plotly_chart(fig)


plot_binance_predictions(filtered_results)

plot_mse(filtered_results)

# ---------------------------- Weather data -----------------------
st.markdown('## Weather information')


def plot_weather_data(df):
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        show_paris = st.checkbox('Paris', value=True)
    with col2:
        show_lon = st.checkbox('London', value=False)
    with col3:
        show_tokyo = st.checkbox('Tokyo', value=False)
    with col4:
        show_wawa = st.checkbox('Warsaw', value=False)
    with col5:
        show_ny = st.checkbox('New York', value=True)

    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=('Temperature', 'Humidity', 'Wind Speed'),
        row_heights=[0.3, 0.3, 0.3]
    )
    if show_paris:
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['tem_paris'],
            mode='lines',
            name='Paris',
            legendgroup='Paris',
            line=dict(color='red'),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['hum_paris'],
            mode='lines',
            name='Paris',
            legendgroup='Paris',
            line=dict(color='red'),
            showlegend=False
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['wind_paris'],
            mode='lines',
            name='Paris',
            legendgroup='Paris',
            line=dict(color='red'),
            showlegend=False
        ), row=3, col=1)

    if show_lon:
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['tem_london'],
            mode='lines',
            name='London',
            legendgroup='London',
            line=dict(color='green'),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['hum_london'],
            mode='lines',
            name='London',
            legendgroup='London',
            line=dict(color='green'),
            showlegend=False
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['wind_london'],
            mode='lines',
            name='London',
            legendgroup='London',
            line=dict(color='green'),
            showlegend=False
        ), row=3, col=1)

    if show_tokyo:
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['tem_tokyo'],
            mode='lines',
            name='Tokyo',
            legendgroup='Tokyo',
            line=dict(color='blue'),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['hum_tokyo'],
            mode='lines',
            name='Tokyo',
            legendgroup='Tokyo',
            line=dict(color='blue'),
            showlegend=False
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['wind_tokyo'],
            mode='lines',
            name='Tokyo',
            legendgroup='Tokyo',
            line=dict(color='blue'),
            showlegend=False
        ), row=3, col=1)

    if show_wawa:
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['tem_wawa'],
            mode='lines',
            name='Warsaw',
            legendgroup='Warsaw',
            line=dict(color='gold'),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['hum_wawa'],
            mode='lines',
            name='Warsaw',
            legendgroup='Warsaw',
            line=dict(color='gold'),
            showlegend=False
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['wind_wawa'],
            mode='lines',
            name='Warsaw',
            legendgroup='Warsaw',
            line=dict(color='gold'),
            showlegend=False
        ), row=3, col=1)

    if show_ny:
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['tem_ny'],
            mode='lines',
            name='New York',
            legendgroup='New York',
            line=dict(color='white'),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['hum_ny'],
            mode='lines',
            name='New York',
            legendgroup='New York',
            line=dict(color='white'),
            showlegend=False
        ), row=2, col=1)
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['wind_ny'],
            mode='lines',
            name='New York',
            legendgroup='New York',
            line=dict(color='white'),
            showlegend=False
        ), row=3, col=1)

    fig.update_layout(
        title='Weather Data Comparison',
        xaxis_title='Date',
        yaxis_title='Value',
        template='plotly_dark',
        xaxis_tickangle=-90,
        xaxis=dict(
            tickformat='%Y/%m/%d %H:%M'
        ),
        legend=dict(
            x=0.5,
            y=1.1,
            traceorder='normal',
            orientation='h',
            xanchor='center',
            yanchor='bottom'
        ),
        height=900
    )

    st.plotly_chart(fig)


plot_weather_data(filtered_results)

# ---------------------------- Sentiment data -----------------------
st.markdown('## Sentiment')


def plot_sentiment(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['sentiment'],
        mode='lines',
        name='Sentiment',
        line=dict(color='white', width=2)
    ))

    fig.update_layout(
        title='Sentiment Analysis Over Time',
        xaxis_title='Date',
        yaxis_title='Sentiment value',
        xaxis_tickangle=-90,
        template='plotly_dark',
        xaxis=dict(
            tickformat='%Y/%m/%d %H:%M'
        ),
        legend=dict(x=0, y=1, traceorder='normal', orientation='h')
    )
    st.plotly_chart(fig)


plot_sentiment(filtered_results)


# ---------------------------- Dataframes -----------------------
st.header("Hive database")

st.write("### Model results")
st.dataframe(model_results)

st.header("HBase database")
st.write("### Reddit posts")
st.dataframe(df_posts)
st.write("### Reddit comments")
st.dataframe(df_comments)