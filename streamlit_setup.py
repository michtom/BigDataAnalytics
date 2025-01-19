import json
from pyhive import hive
import happybase
import pandas as pd
import streamlit as st


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
