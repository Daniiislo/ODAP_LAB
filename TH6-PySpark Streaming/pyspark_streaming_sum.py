from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
import shutil
import os

spark = SparkSession.builder \
    .appName("PySpark Streaming Sum") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Clean up checkpoint directory BEFORE creating SparkSession
checkpoint_dir = "checkpoint"
if os.path.exists(checkpoint_dir):
    shutil.rmtree(checkpoint_dir)
    print(f"Cleaned checkpoint directory: {checkpoint_dir}")

# Define the inputs source
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into numbers by space
numbers = lines.select(split(col("value"), "\\s+").alias("numbers"))

# Explode the array of numbers into individual rows
exploded_numbers = numbers.select(explode(col("numbers")).alias("number"))

# Validate and convert the numbers to integers, filtering out non-numeric values
valid_numbers = exploded_numbers \
    .filter(col("number") != "") \
    .filter(col("number").rlike("^-?\\d+$")) \
    .select(col("number").cast(IntegerType()).alias("integer_number"))

# Add a dummy key for groupBy
valid_numbers_with_key = valid_numbers.withColumn("key", lit(1))

# Calculate the sum of the valid numbers
sum_result = valid_numbers_with_key.groupBy("key") \
    .agg(sum("integer_number").alias("total_sum")) \
    .select("total_sum")

# Write the result to the console 
streaming_query = sum_result.writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime="1 seconds") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

try:
    # Wait for the streaming query to finish
    streaming_query.awaitTermination()
except KeyboardInterrupt:
    print("\nReceived interrupt signal")
finally:
    print("Cleaning up...")
    if 'streaming_query' in locals():
        streaming_query.stop()
    spark.stop()