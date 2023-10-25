from pyspark.sql import SparkSession
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

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

# Convert PySpark DataFrame to Avro format
avro_data = df.select("field1", "field2").rdd.map(lambda row: (row.field1, row.field2))

# Configure Kafka Avro Producer
producer_config = {
    'bootstrap.servers': 'your_kafka_broker',
    'schema.registry.url': 'http://schema-registry-url',  # URL of the Avro schema registry
}

producer = AvroProducer(producer_config, default_value_schema=avro_schema)

# Define a callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Publish Avro records to a Kafka topic
for avro_record in avro_data.collect():
    key = None  # You can set a key if needed
    producer.produce(topic='your_topic', key=key, value=avro_record, value_schema=avro_schema, callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
