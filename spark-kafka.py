from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split, udf, avg
from pyspark.sql.types import *
from pyspark import SparkContext, SQLContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_squared_error
from datetime import datetime
import numpy as np
import joblib
import os
import json
import happybase

import findspark
findspark.init()

# -------------- SETUP --------------

# Suppress Spark INFO logs
import logging
logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)
spark_logger = logging.getLogger("org.apache.spark")
spark_logger.setLevel(logging.ERROR)

# Define the schema for the incoming data
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", TimestampType())

# Initialize Spark Session
spark = SparkSession.builder.appName("KafkaSparkIntegrationWithML") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .enableHiveSupport() \
    .getOrCreate()

# Set log level for Spark to ERROR
spark.sparkContext.setLogLevel("ERROR")

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "binance_marketdata") \
    .option("startingOffsets", "latest").load()

# Process the data
df_processed = df.selectExpr("CAST(value as STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp")).drop("timestamp")

df_processed = df_processed.withColumn("base", split(col("symbol"), "USDT")[0]) \
    .withColumn("quote", split(col("symbol"), "USDT")[1]) \
    .withColumn("quote", col("quote").cast(StringType()).alias("quote"))

# Sentiment analyzer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = spark.sparkContext.broadcast(SentimentIntensityAnalyzer())

# Path to store the model
model_path = "/home/vagrant/BigDataAnalytics/models/sgd_model.pkl"

# Load or initialize the model
if os.path.exists(model_path):
    model = joblib.load(model_path)
else:
    model = SGDRegressor(max_iter=1, warm_start=True)


# -------------- FUNCTIONS --------------
def get_weather_data():
    weather_date_time = datetime.now().strftime("%Y-%m-%d-%H")
    weather_json_path = f"hdfs://localhost:8020/user/big_d_analytics/weather/weather_{weather_date_time}.json"
    spark_df = spark.read.json(weather_json_path)
    processed_spark_df = spark_df.select(
        col("name").alias("city_name"),
        col("sys.country").alias("country"),
        col("main.temp").alias("temperature"),
        col("main.humidity").alias("humidity"),
        col("wind.speed").alias("wind_speed")
    )
    # change temperature in kelwins to celcius
    processed_spark_df = processed_spark_df.withColumn("temperature", col("temperature") - 273.15)
    
    # prepare weather data
    pandas_df = processed_spark_df.toPandas()
    pandas_df = pandas_df.sort_values(by=['country'])
    pandas_df = pandas_df.drop(["city_name", "country"], axis=1)
    temp = pandas_df.loc[:, "temperature"].to_numpy()
    hum = pandas_df.loc[:, "humidity"].to_numpy()
    wind = pandas_df.loc[:, "wind_speed"].to_numpy()
    weather_data = np.concatenate([temp, hum, wind])
    
    return weather_data

def get_comment_score(comments):
    global analyzer
    sentiments = []
    upvotes = []
    
    if not comments: # empty
        return float(0)
    for comment in comments:
        sentiments.append(analyzer.value.polarity_scores(comment.comm_body)['compound'])
        upvotes.append(comment.comm_score if comment.comm_score > 0 else 0)
        
    return float(np.mean(np.array(sentiments) * np.array(upvotes) / np.sum(upvotes)))

def get_post_score(title):
    global analyzer
    return float(analyzer.value.polarity_scores(title)['compound'])

def convert_row(row):
    return str(row['post_id']).encode(), {
        b"post_info:post_title": str(row['post_title']).encode(),
        b"post_info:post_time": str(row['post_time']).encode(),
        b"post_info:post_score": str(row['post_score']).encode(),
        b"post_info:post_upvote": str(row['post_upvote']).encode(),
        b"post_info:comments": json.dumps([comment.asDict() for comment in row['comments']]).encode(),
        b"sentiment:post_sentiment": str(row['post_sentiment']).encode(),
        b"sentiment:comments_sentiment": str(row['comments_sentiment']).encode()
    }

def write_to_hbase(rows):
    VM_address = '127.0.0.1'
    connection = happybase.Connection(VM_address)
    table = connection.table('reddit_sentiment')

    # Insert rows into HBase
    with table.batch() as batch:
        for row_key, data in rows:
            batch.put(row_key, data)
    connection.close()

def get_reddit_sentiment_data():
    fallback_schema = StructType([
        StructField("_corrupt_record", StringType(), True)  # Capture malformed rows
    ])
    
    schema = StructType([
        StructField("post_id", StringType(), True),
        StructField("post_title", StringType(), True),
        StructField("post_time", DoubleType(), True),
        StructField("post_score", IntegerType(), True),
        StructField("post_upvote", DoubleType(), True),
        StructField("comments", ArrayType(
            StructType([
                StructField("comm_id", StringType(), True),
                StructField("comm_body", StringType(), True),
                StructField("comm_time_utc", DoubleType(), True),
                StructField("comm_score", IntegerType(), True)
            ])
        ), True)
    ])

    reddit_date_time = datetime.now().strftime("%Y-%m-%d-%H")
    reddit_json_path = f"hdfs://localhost:8020/user/big_d_analytics/reddit/posts_got_{reddit_date_time}.json"
    # json_path = "hdfs://localhost:8020/user/big_d_analytics/reddit/posts_got_2025-01-07-11.json"
    df = spark.read.schema(schema).json(reddit_json_path, multiLine=True)

    comment_sentiment_udf = udf(get_comment_score, DoubleType())
    post_sentiment_udf = udf(get_post_score, DoubleType())
    
    df_with_sentiment = df.withColumn("comments_sentiment", comment_sentiment_udf(col("comments")))
    df_with_sentiment = df_with_sentiment.withColumn("post_sentiment", post_sentiment_udf(col("post_title")))

    # Convert DataFrame rows to HBase format
    rows = df_with_sentiment.rdd.map(lambda row: convert_row(row.asDict())).collect()
    # Write rows to HBase
    write_to_hbase(rows)

    aggragated_sentiment = df_with_sentiment.agg(avg("post_sentiment")).collect()[0][0]
    
    return aggragated_sentiment

# Function to process each batch
def train_model(batch_df, batch_id):
    global X_prev_1
    global X_prev_2
    global weather_date_time
    global weather_data
    global reddit_date_time
    global reddit_data
    
    if weather_date_time != datetime.now().strftime("%Y-%m-%d-%H"):
        try:
            weather_date_time = datetime.now().strftime("%Y-%m-%d-%H")
            weather_data = get_weather_data()
        except:
            pass

    if reddit_date_time != datetime.now().strftime("%Y-%m-%d-%H"):
        try:
            reddit_date_time = datetime.now().strftime("%Y-%m-%d-%H")
            reddit_data = get_reddit_sentiment_data()
        except:
            pass
    
    if batch_df.count() > 0:
        # Convert Spark DataFrame to Pandas DataFrame
        batch_data = batch_df.select("price").toPandas()
        
        # Prepare features and labels
        if X_prev_1 is None:
            X_prev_1 = batch_data["price"].values.reshape(-1, 1)
            pass
        if X_prev_2 is None:
            X_prev_2 = X_prev_1
            X_prev_1 = batch_data["price"].values.reshape(-1, 1)
            pass
        
        X = X_prev_1 - X_prev_2
        y = (batch_data["price"].values - X_prev_1).flatten()
        X = np.concatenate([X, weather_data.reshape(1, -1), np.array([[reddit_data]])], axis=1)
        X = np.tile(X, (y.shape[0],1))
        
        # Predict on current batch
        if hasattr(model, "coef_") and model.coef_ is not None:  # Ensure the model is trained before predicting
            y_pred = model.predict(X)
            mse = mean_squared_error(y, y_pred)
            print(f"Batch {batch_id} - Mean Squared Error: {mse}")
        else:
            print(f"Batch {batch_id} - Model not yet trained, skipping metric calculation.")
        
        # Train the model incrementally
        model.partial_fit(X, y)
        # Update previous observation
        X_prev_1 = batch_data["price"].values[-1:].reshape(-1, 1)
        X_prev_2 = X_prev_1
        # Save the model
        joblib.dump(model, model_path)
        print(f"Model updated and saved at {model_path}")

        X_hive = X[0].tolist()
        X_hive.append(y[0])
        X_hive.append(y_pred[0])
        X_hive.append("{:.0f}".format(mse))
        X_hive.append(datetime.timestamp(datetime.now()))
        X_hive = [str(el) for el in X_hive]
        query = "INSERT INTO TABLE model_results VALUES ("+', '.join(X_hive)+")"
        spark.sql(query)
        
# -------------- GLOBAL VARIABLES --------------
# Global variables
X_prev_1 = None
X_prev_2 = None
weather_date_time = datetime.now().strftime("%Y-%m-%d-%H")
weather_data = get_weather_data()

reddit_date_time = datetime.now().strftime("%Y-%m-%d-%H")
reddit_data = get_reddit_sentiment_data()


# -------------- MAIN PART OF SCRIPT --------------
# Apply the ML training function to the streaming data
query = df_processed.writeStream \
    .foreachBatch(train_model) \
    .outputMode("append") \
    .start()

# Await termination
spark.streams.awaitAnyTermination()