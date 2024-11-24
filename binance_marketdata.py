from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split, window, avg

from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", TimestampType())

spark = SparkSession.builder.appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "binance_marketdata") \
    .option("startingOffsets", "latest").load()

df_processed = df.selectExpr("CAST(value as STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp")).drop("timestamp")

#df_aggregated = df_processed.groupBy(window(col("event_time"), "1 minute"),
#                                     col("symbol"),
#                                     ).agg(avg("price").alias("avg_price"))

df_processed = df_processed.withColumn("base", split(col("symbol"), "USDT")[0]) \
    .withColumn("quote", split(col("symbol"), "USDT")[1]) \
    .withColumn("quote", col("quote").cast(StringType()).alias("quote"))

#query = df_aggregated.writeStream.format("console").outputMode("complete").start()

query = df_processed.writeStream.format("parquet").option("format", "append") \
    .option("path", "/home/vagrant/big_data/binance_marketdata/csv/") \
    .option("checkpointLocation", "/home/vagrant/big_data/binance_marketdata/checkpoints/") \
    .queryName("binance") \
    .outputMode("append").start()

spark.streams.awaitAnyTermination()
