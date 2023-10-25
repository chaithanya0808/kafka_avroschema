from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from abris.avro import to_avro

# Define your Avro schema
avro_schema = {
    "type": "record",
    "name": "example",
    "fields": [
        {"name": "field1", "type": "string"},
        {"name": "field2", "type": "int"}
    ]
}

# Initialize a Spark session
spark = SparkSession.builder.appName("KafkaAvroExample").getOrCreate()

# Load your data into a PySpark DataFrame
data = [("value1", 1), ("value2", 2)]
columns = ["field1", "field2"]
df = spark.createDataFrame(data, columns)

# Convert PySpark DataFrame to Avro format using ABRiS
avro_df = df.select("field1", "field2")

# Convert the DataFrame to Avro format
avro_records = avro_df.select(to_avro("*", avro_schema).alias("value"))

# Configure Kafka Avro Producer
producer_config = {
    'bootstrap.servers': 'your_kafka_broker',
    'key.serializer': 'org.apache.kafka.common.serialization.StringSerializer',
    'value.serializer': 'org.apache.kafka.common.serialization.ByteArraySerializer',  # Avro data is serialized as bytes
}

# Create a DataFrame with a key and value for Kafka
kafka_df = avro_records.withColumn("key", col("field1"))

# Send the DataFrame to Kafka
kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", producer_config['bootstrap.servers']) \
    .option("kafka.key.serializer", producer_config['key.serializer']) \
    .option("kafka.value.serializer", producer_config['value.serializer']) \
    .option("topic", "your_topic") \
    .save()

# Note that ABRiS handles Avro serialization automatically
