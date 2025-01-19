import json
from pyhive import hive
import happybase
import pandas as pd
import streamlit as st
from hdfs import InsecureClient


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


st.title("BigD Analytics - first test version of dashboard")

query = "SELECT * FROM model_results"

st.header("Hive database")

if st.button("Load Hive data"):
    try:
        model_results = get_hive_data(query)
        st.write("### Hive Data")
        st.dataframe(model_results)
    except Exception as e:
        st.error(f"Error while downloading Hive data: {e}")

st.header("HBase database")

if st.button("Load HBase data"):
    try:
        df_posts, df_comments = get_hbase_reddit_data()
        st.write("### HBase Data")
        st.dataframe(df_posts)
        st.dataframe(df_comments)
    except Exception as e:
        st.error(f"Error while downloading HBase data: {e}")

st.header("HDFS data")

if st.button("Load HDFS data"):
    try:
        df_prices = get_binance_marketdata()
        st.write("### Binance Marketdata")
        st.dataframe(df_prices)
    except Exception as e:
        st.error(f"Error while downloading HDFS data: {e}")
