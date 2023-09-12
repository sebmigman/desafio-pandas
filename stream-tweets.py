from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .master("local[*]") \
    .getOrCreate()

# Define Kafka-related configurations
kafka_bootstrap_servers = "localhost:29092"  # Change to your Kafka broker address
kafka_topic = "test"  # Change to your Kafka topic

# Define the Kafka data source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()
## schema

tweet_schema = (
        StructType()
        .add("glossary", 
             StructType().add("title", StringType())
             )
    )

# Assuming you have six aggregation functions q1, q2, q3, q4, q5, and q6
# You can apply them to the streaming DataFrame


base_df = kafka_df.selectExpr("CAST(value AS STRING) as value", "timestamp") 

info_dataframe = base_df.select(
        from_json(col("value"), tweet_schema).alias("sample"), "timestamp"
    )

info_df_fin = info_dataframe.select("sample.*", "timestamp")

# Define a query to output the results to the console
query = info_df_fin \
    .writeStream \
    .format("console") \
    .start()

# Start the streaming query
query.awaitTermination()
