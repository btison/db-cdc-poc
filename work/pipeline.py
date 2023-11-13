# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

TOPIC = spark.conf.get("TOPIC")
KAFKA_BROKER = spark.conf.get("KAFKA_BROKER")
SECURITY_PROTOCOL = spark.conf.get("KAFKA_SECURITY_PROTOCOL")
SASL_MECHANISM = spark.conf.get("KAFKA_SASL_MECHANISM")
CLIENT_ID = spark.conf.get("KAFKA_CLIENT_ID")
CLIENT_SECRET = spark.conf.get("KAFKA_CLIENT_SECRET")

raw_kafka_events = (spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("kafka.security.protocol", SECURITY_PROTOCOL)
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(CLIENT_ID, CLIENT_SECRET))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", SASL_MECHANISM)
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
    )

reset = "true"

@dlt.table(comment="The data ingested from kafka topic",
           table_properties={"pipelines.reset.allowed": reset}
          )
def kafka_events():
  return raw_kafka_events

# COMMAND ----------

@dlt.table(comment="raw kafka events",
           table_properties={"pipelines.reset.allowed": reset},
           temporary=False)

def kafka_raw():
  return (
    # kafka streams are (key,value) with value containing the kafka payload, i.e. event
    # no automatic schema inference!
    dlt.read_stream("kafka_events")
    .select(col("key").cast("string"),col("value").cast("string"))
  )

# COMMAND ----------

# {
#    "before": null,
#    "after": {
#       "id": 3,
#       "user_id": "jcarroll",
#       "first_name": "Jaxon",
#       "last_name": "Carroll",
#       "email": "jcarroll@yihaa.com",
#       "phone": "(912) 407-4028"
#    },
#    "source": {
#       "version": "2.1.1.Final",
#       "connector": "postgresql",
#       "name": "globex.updates",
#       "ts_ms": 1699609517798,
#       "snapshot": "true",
#       "db": "globex",
#       "sequence": "[null,\"24050752\"]",
#       "schema": "public",
#       "table": "customer",
#       "txId": 1180,
#       "lsn": 24050752,
#       "xmin": null
#    },
#    "op": "r",
#    "ts_ms": 1699609518081,
#    "transaction": null
# }




key_schema = StructType([ \
    StructField("id", StringType(),True), \
  ])

value_schema = StructType([ \
    StructField("before", StructType(),True), \
    StructField("after", StructType([StructField("id", LongType()), StructField("user_id", StringType()), StructField("first_name", StringType()), StructField("last_name", StringType()), StructField("email", StringType()), StructField("phone", StringType())]),True), \
    StructField("source", StructType(),True), \
    StructField("op", StringType(),True), \
    StructField("ts_ms", LongType(),True), \
    StructField("transaction", StringType(),True), \
  ])

# COMMAND ----------

@dlt.table(comment="data extracted from debezium payload",
           table_properties={"pipelines.reset.allowed": reset},
           temporary=False)
def kafka_data_extracted():
    return(
        dlt.read_stream("kafka_raw")
        .select(from_json(col("key"), key_schema).alias("key"), from_json(col("value"), value_schema).alias("value"))
        .select("key.id", "value.ts_ms", "value.op" , "value.after.user_id", "value.after.first_name", "value.after.last_name", "value.after.email", "value.after.phone")
    )
