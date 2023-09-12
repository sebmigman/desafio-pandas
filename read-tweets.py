from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaToConsole") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:29092",  # Change to your Kafka broker address
    "subscribe": "test"  # Change to the Kafka topic you want to read
}

# Read data from Kafka topic
raw_stream_data = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Convert the value column from Kafka into a string
stream_data = raw_stream_data.selectExpr("CAST(value AS STRING)")

# Print the data to the console
query = stream_data \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to terminate (you can also use query.awaitTermination(timeoutInSeconds) with a timeout)
query.awaitTermination()
