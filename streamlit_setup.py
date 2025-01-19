import json
from pyhive import hive
import happybase
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta


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

model_results = get_hive_data(query)
st.write("### Hive Data")
st.dataframe(model_results)

#st.header("HBase database")

#df_posts, df_comments = get_hbase_reddit_data()
#st.write("### HBase Data")
#st.dataframe(df_posts)
#st.dataframe(df_comments)


# ---------------------------- Change hive dataframe -----------------------

# For printing on plots
model_results['date'] = pd.to_datetime(model_results['model_results.time_stamp'], unit='s')
model_results.columns = model_results.columns.str.replace('model_results.', '')
# Sort (not necessary, but easier to look at)
model_results = model_results.sort_values('time_stamp').reset_index(drop=True)


# ---------------------------- Time slider for everything -----------------------
st.markdown('## Select time period')

# Slider to filter data by date
# TODO: fix labels or sth
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

# ---------------------------- Binance values and predictions -----------------------

st.markdown('## Model predictions')

# Filter data based on slider
filtered_results = model_results[(model_results['time_stamp'] >= datetime.timestamp(start)) & (model_results['time_stamp'] <= datetime.timestamp(end))]

# Plot the filtered data
fig, ax = plt.subplots()
ax.plot(filtered_results['date'], filtered_results['y'], label='True', color='blue', linewidth=0.5)
ax.plot(filtered_results['date'], filtered_results['y_hat'], label='Prediction', color='red', ls='--')
ax.set_xlabel('Date')
ax.set_ylabel('Bitcoin price change [USD]')
ax.set_title('Binance Prediction')
plt.xticks(rotation=90)
ax.legend()
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M'))

# Display the plot
st.pyplot(fig)


# ---------------------------- Weather data -----------------------
st.markdown('## Weather information')

# Select towns
col1, col2, col3, col4, col5 = st.columns(5)

# Place checkboxes in the columns
with col1:
    show_paris = st.checkbox('Paris', value=True)
with col2:
    show_lon = st.checkbox('London', value=True)
with col3:
    show_tokyo = st.checkbox('Tokyo', value=True)
with col4:
    show_wawa = st.checkbox('Warsaw', value=True)
with col5:
    show_ny = st.checkbox('New York', value=True)

fig, ax = plt.subplots(ncols=1, nrows=3, figsize=(10, 15))

# Plot data
if show_paris:
    ax[0].plot(filtered_results['date'], filtered_results['tem_paris'], color='red', linewidth=1, label='Paris')
    ax[1].plot(filtered_results['date'], filtered_results['hum_paris'], color='red', linewidth=1, label='Paris')
    ax[2].plot(filtered_results['date'], filtered_results['wind_paris'], color='red', linewidth=1, label='Paris')
if show_lon:
    ax[0].plot(filtered_results['date'], filtered_results['tem_london'], color='green', linewidth=1, label='London')
    ax[1].plot(filtered_results['date'], filtered_results['hum_london'], color='green', linewidth=1, label='London')
    ax[2].plot(filtered_results['date'], filtered_results['wind_london'], color='green', linewidth=1, label='London')
if show_tokyo:
    ax[0].plot(filtered_results['date'], filtered_results['tem_tokyo'], color='blue', linewidth=1, label='Tokyo')
    ax[1].plot(filtered_results['date'], filtered_results['hum_tokyo'], color='blue', linewidth=1, label='Tokyo')
    ax[2].plot(filtered_results['date'], filtered_results['wind_tokyo'], color='blue', linewidth=1, label='Tokyo')
if show_wawa:
    ax[0].plot(filtered_results['date'], filtered_results['tem_wawa'], color='gold', linewidth=1, label='Warsaw')
    ax[1].plot(filtered_results['date'], filtered_results['hum_wawa'], color='gold', linewidth=1, label='Warsaw')
    ax[2].plot(filtered_results['date'], filtered_results['wind_wawa'], color='gold', linewidth=1, label='Warsaw')
if show_ny:
    ax[0].plot(filtered_results['date'], filtered_results['tem_ny'], color='black', linewidth=1, label='New York')
    ax[1].plot(filtered_results['date'], filtered_results['hum_ny'], color='black', linewidth=1, label='New York')
    ax[2].plot(filtered_results['date'], filtered_results['wind_ny'], color='black', linewidth=1, label='New York')

# Add legend (top)
handles, labels = ax[0].get_legend_handles_labels()
fig.legend(handles, labels, loc='upper center', ncol=5, labelspacing=0.1, bbox_to_anchor=(0.5, 0.95))

# Add titles, labels etc.
ax[0].set_title('Temperature')
ax[0].set_ylabel('Temperature [C]')

ax[1].set_title('Humidity')
ax[1].set_ylabel('Humidity [%]')

ax[2].set_title('Wind speed')
ax[2].set_ylabel('Wind speed [m/s]')

ax[0].tick_params(rotation=90)
ax[1].tick_params(rotation=90)
ax[2].tick_params(rotation=90)

ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M'))
ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M'))
ax[2].xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M'))

plt.subplots_adjust(hspace=0.65)

st.pyplot(fig)

# ---------------------------- Sentiment data -----------------------
st.markdown('## Sentiment')

# Plot the filtered data
fig, ax = plt.subplots()
ax.plot(filtered_results['date'], filtered_results['sentiment'], color='black', linewidth=1)
ax.set_xlabel('Date')
ax.set_ylabel('Sentiment value')
plt.xticks(rotation=90)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d %H:%M'))

# Display the plot
st.pyplot(fig)