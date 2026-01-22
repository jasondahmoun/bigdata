import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType, StringType, IntegerType

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'weather_data')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'hdfs-namenode:9000')

spark = SparkSession.builder \
    .appName("WeatherAggregation") \
    .master(SPARK_MASTER) \
    .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("temperature", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("winddirection", IntegerType(), True),
    StructField("weathercode", IntegerType(), True),
    StructField("is_day", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("interval", IntegerType(), True)
])

raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
parsed = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

parsed = parsed.withColumn("high_wind_alert", col("windspeed") > 10)

agg = parsed.groupBy().agg(
    avg("temperature").alias("avg_temp_c"),
    count(col("high_wind_alert")).alias("alert_count")
)

print("\n=== Agrégation Météo ===")
agg.show(truncate=False)

hdfs_path = f"hdfs://{HDFS_NAMENODE}/weather_data/aggregation"

agg.coalesce(1).write.mode("overwrite").option("header", "true").csv(hdfs_path)

print(f"\n✅ Données sauvegardées dans HDFS: {hdfs_path}")
print(f"📊 Accédez à l'interface HDFS: http://localhost:9870")
print(f"   Chemin: /weather_data/aggregation/")

spark.stop()
