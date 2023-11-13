# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

TOPIC_CUSTOMER = spark.conf.get("TOPIC_CUSTOMER")
TOPIC_ADDRESS = spark.conf.get("TOPIC_ADDRESS")
KAFKA_BROKER = spark.conf.get("KAFKA_BROKER")
SECURITY_PROTOCOL = spark.conf.get("KAFKA_SECURITY_PROTOCOL")
SASL_MECHANISM = spark.conf.get("KAFKA_SASL_MECHANISM")
CLIENT_ID = spark.conf.get("KAFKA_CLIENT_ID")
CLIENT_SECRET = spark.conf.get("KAFKA_CLIENT_SECRET")

reset = "true"

# COMMAND ----------

raw_kafka_events_customer = (spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC_CUSTOMER)
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("kafka.security.protocol", SECURITY_PROTOCOL)
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(CLIENT_ID, CLIENT_SECRET))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", SASL_MECHANISM)
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
    )

@dlt.table(comment="The data ingested from kafka topic",
           table_properties={"pipelines.reset.allowed": reset}
          )
def kafka_events_customer():
  return raw_kafka_events_customer

# COMMAND ----------

@dlt.table(comment="raw kafka events_customer",
           table_properties={"pipelines.reset.allowed": reset},
           temporary=True)

def kafka_raw_customer():
  return (
    # kafka streams are (key,value) with value containing the kafka payload, i.e. event
    # no automatic schema inference!
    dlt.read_stream("kafka_events_customer")
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


key_schema_customer = StructType([ \
    StructField("id", LongType(),True), \
  ])

value_schema_customer = StructType([ \
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
def kafka_data_extracted_customer():
    return(
        dlt.read_stream("kafka_raw_customer")
        .select(from_json(col("key"), key_schema_customer).alias("key"), from_json(col("value"), value_schema_customer).alias("value"))
        .select("key.id", "value.ts_ms", "value.op" , "value.after.user_id", "value.after.first_name", "value.after.last_name", "value.after.email", "value.after.phone")
    )

# COMMAND ----------

dlt.create_target_table(
    name = "customer_cdc")

dlt.apply_changes(
    target = "customer_cdc",
    source = "kafka_data_extracted_customer",
    keys = ["id"],
    sequence_by = col("ts_ms"),
    apply_as_deletes = expr("op = 'd'"),
    except_column_list = ["op"])

# COMMAND ----------

raw_kafka_events_address = (spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC_ADDRESS)
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("kafka.security.protocol", SECURITY_PROTOCOL)
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(CLIENT_ID, CLIENT_SECRET))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", SASL_MECHANISM)
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
    )

@dlt.table(comment="The data ingested from kafka topic",
           table_properties={"pipelines.reset.allowed": reset}
          )
def kafka_events_address():
  return raw_kafka_events_address

# COMMAND ----------

@dlt.table(comment="raw kafka events address",
           table_properties={"pipelines.reset.allowed": reset},
           temporary=True)

def kafka_raw_address():
  return (
    # kafka streams are (key,value) with value containing the kafka payload, i.e. event
    # no automatic schema inference!
    dlt.read_stream("kafka_events_address")
    .select(col("key").cast("string"),col("value").cast("string"))
  )

# COMMAND ----------

# {
#    "before": null,
#    "after": {
#       "cust_id": 1,
#       "address1": "28 Cedar Crest",
#       "address2": "",
#       "city": "Gresham",
#       "zip": "68367",
#       "state": "NE",
#       "country": "USA"
#    },
#    "source": {
#       "version": "2.1.1.Final",
#       "connector": "postgresql",
#       "name": "globex.updates",
#       "ts_ms": 1699869306488,
#       "snapshot": "first_in_data_collection",
#       "db": "globex",
#       "sequence": "[null,\"24050752\"]",
#       "schema": "public",
#       "table": "address",
#       "txId": 1180,
#       "lsn": 24050752,
#       "xmin": null
#    },
#    "op": "r",
#    "ts_ms": 1699869306973,
#    "transaction": null
# }

key_schema_address = StructType([ \
    StructField("cust_id", LongType(),True), \
  ])

value_schema_address = StructType([ \
    StructField("before", StructType(),True), \
    StructField("after", StructType([StructField("cust_id", LongType()), StructField("address1", StringType()), StructField("address2", StringType()), StructField("city", StringType()), StructField("zip", StringType()), StructField("state", StringType()), StructField("country", StringType())]),True), \
    StructField("source", StructType(),True), \
    StructField("op", StringType(),True), \
    StructField("ts_ms", LongType(),True), \
    StructField("transaction", StringType(),True), \
  ])

# COMMAND ----------

@dlt.table(comment="data extracted from debezium payload",
           table_properties={"pipelines.reset.allowed": reset},
           temporary=False)
def kafka_data_extracted_address():
    return(
        dlt.read_stream("kafka_raw_address")
        .select(from_json(col("key"), key_schema_address).alias("key"), from_json(col("value"), value_schema_address).alias("value"))
        .select("key.cust_id", "value.ts_ms", "value.op" , "value.after.address1", "value.after.address2", "value.after.city", "value.after.zip", "value.after.state", "value.after.country")
    )

# COMMAND ----------

dlt.create_target_table(
    name = "address_cdc")

dlt.apply_changes(
    target = "address_cdc",
    source = "kafka_data_extracted_address",
    keys = ["cust_id"],
    sequence_by = col("ts_ms"),
    apply_as_deletes = expr("op = 'd'"),
    except_column_list = ["op"])

# COMMAND ----------

# Join the customer table and address table
@dlt.table(
  comment="Joining Tables"
)
def customer_gold_table():
  customer = dlt.read("customer_cdc")
  address = dlt.read("address_cdc")
  return ( 
     customer.join(address, customer.id == address.cust_id, how="inner").select(customer.id, customer.user_id, customer.first_name, customer.last_name, customer.email, customer.phone, address.address1, address.address2, address.city, address.zip, address.state, address.country)
  )

