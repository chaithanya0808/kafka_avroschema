%%writefile sample-code/src/spark-streaming-kafka-producer.py
import json
from io import BytesIO
import os
import avro.schema
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import struct
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

...

df = spark.read.option("header", "true").schema(schema).csv(
    os.getenv("INPUT_PATH", "s3a://openlake/spark/sample-data/taxi-data.csv"))
df = df.select(to_avro(struct([df[x] for x in df.columns]), value_schema_str).alias("value"))

df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 
            os.getenv("KAFKA_BOOTSTRAM_SERVER", "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092")) \
    .option("flushInterval", "100ms") \
    .option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") \
    .option("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer") \
    .option("schema.registry.url",
            os.getenv("KAFKA_SCHEMA_REGISTRY", "http://kafka-schema-registry-cp-schema-registry.kafka.svc.cluster.local:8081")) \
    .option("topic", "nyc-avro-topic") \
    .save()
