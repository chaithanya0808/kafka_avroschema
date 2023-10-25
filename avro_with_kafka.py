from pyspark.sql import SparkSession
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.avro import AvroProducer, AvroConsumer
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

# Define your Avro schema
avro_schema = {
    "type": "record",
    "name": "example",
    "fields": [
        {"name": "field1", "type": "string"},
        {"name": "field2", "type": "int"},
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

# Configure Kafka Producer with Avro serializer
producer_config = {
    'bootstrap.servers': 'your_kafka_broker',
    'schema.registry.url': 'http://schema-registry-url',  # URL of the Avro schema registry
    'key.serializer': 'your_key_serializer',  # Specify the key serializer
    'value.serializer': 'avro',  # Avro serializer
}

producer = AvroProducer(producer_config)

# Publish Avro records to a Kafka topic
for avro_record in avro_data.collect():
    key = 'your_key'
    producer.produce(topic='your_topic', key=key, value=avro_record, value_schema=avro_schema)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()

# On the consumer side, you can use the Confluent Kafka Avro Consumer to deserialize Avro records and process them.
consumer_config = {
    'bootstrap.servers': 'your_kafka_broker',
    'group.id': 'your_consumer_group',
    'auto.offset.reset': 'earliest',
    'key.deserializer': 'your_key_deserializer',  # Specify the key deserializer
    'value.deserializer': 'avro',  # Avro deserializer
    'schema.registry.url': 'http://schema-registry-url',  # URL of the Avro schema registry
}

consumer = AvroConsumer(consumer_config)

consumer.subscribe(['your_topic'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f"Reached end of partition {msg.partition()}")
        else:
            print(f"Error while polling message: {msg.error()}")
    else:
        print(f"Received message: {msg.value()}")
