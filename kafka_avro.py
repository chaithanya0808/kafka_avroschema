from pyspark.sql import SparkSession
from pyspark.sql.functions import from_avro, to_avro
import json
from confluent_kafka import Producer

# Create a Spark session
spark = SparkSession.builder.appName("KafkaExample").getOrCreate()

# Load your DataFrame
df = spark.read.csv("your_data.csv")

# Define the original and new Avro schemas as JSON strings
original_avro_schema = {...}  # Your current schema
new_avro_schema = {...}       # Schema with backward-compatible changes

# Apply original Avro schema to DataFrame
df_avro = df.select(from_avro(df["your_avro_column"], original_avro_schema).alias("avro_data"))

# Ensure compatibility with the new schema
df_avro = df_avro.withColumn("new_field", df_avro["existing_field"])
# You can add more transformations as needed

# Serialize DataFrame to Avro with the new schema
df_avro = df_avro.select(to_avro("avro_data", new_avro_schema).alias("value"))

# Configure Kafka producer with Schema Registry settings
producer = Producer({
    'bootstrap.servers': 'your_kafka_brokers',
    'schema.registry.url': 'your_schema_registry_url',
    'value.subject.name.strategy': 'io.confluent.kafka.serializers.subject.TopicNameStrategy'
})

# Produce to Kafka topic
for row in df_avro.collect():
    producer.produce('your_kafka_topic', value=row.value)

# Close the producer and Spark session
producer.flush()
producer.close()
spark.stop()
