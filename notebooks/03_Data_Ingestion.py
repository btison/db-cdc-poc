# Databricks notebook source
# DBTITLE 1,Import Required Libraries
import pyspark.sql.functions as f
from pyspark.sql.types import *

from delta.tables import *

import dlt # this is the delta live tables library

import time

# COMMAND ----------

# DBTITLE 1,Initialize Configuration
config = {}

# COMMAND ----------

# DBTITLE 1,Config Values
# mount point associated with our data files
config['dbfs_mount_name'] = f'/mnt/pos'

# static data files
config['stores_filename'] = config['dbfs_mount_name'] + '/store.txt'
config['items_filename'] = config['dbfs_mount_name'] + '/item_1000.txt'
config['change_types_filename'] = config['dbfs_mount_name'] + '/inventory_change_type.txt'

config['events_kafka_topic'] = 'inventory.event'
config['cdc_kafka_topic'] = 'retail.updates.public.inventory'
config['kafka_bootstrap_server'] = dbutils.secrets.get(scope = "kafka", key = "bootstrap-server")
config['kafka_sasl'] = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(dbutils.secrets.get(scope = "kafka", key = "username"), dbutils.secrets.get(scope = "kafka", key = "password"))

# COMMAND ----------

# MAGIC %md ## Step 1: Setup the POS Database Environment
# MAGIC
# MAGIC The typical first step in setting up a streaming architecture is to create a database to house our tables.  This needs to be done in advance of running the DLT jobs:
# MAGIC
# MAGIC ```
# MAGIC CREATE DATABASE IF NOT EXISTS pos_dlt;
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md Step 2: Load the Static Reference Data
# MAGIC

# COMMAND ----------

# DBTITLE 1,Stores
# define schema for incoming file
store_schema = StructType([
  StructField('store_id', IntegerType()),
  StructField('name', StringType())
  ])

#define the dlt table
@dlt.table(
  name='store', # name of the table to create
  comment = 'data associated with individual store locations', # description
  table_properties = {'quality': 'silver'}, # various table properties
  spark_conf = {'pipelines.trigger.interval':'24 hours'} # various spark configurations
  )
def store():
  df = (
      spark
      .read
      .csv(
        config['stores_filename'], 
        header=True, 
        schema=store_schema
        )
      )
  return df


# COMMAND ----------

# DBTITLE 1,Items
item_schema = StructType([
  StructField('item_id', IntegerType()),
  StructField('name', StringType()),
  StructField('supplier_id', IntegerType()),
  StructField('safety_stock_quantity', IntegerType())
  ])

@dlt.table(
  name = 'item',
  comment = 'data associated with individual items',
  table_properties={'quality':'silver'},
  spark_conf={'pipelines.trigger.interval':'24 hours'}
)
def item():
  return (
    spark
      .read
      .csv(
        config['items_filename'], 
        header=True, 
        schema=item_schema
        )
  )


# COMMAND ----------

# DBTITLE 1,Inventory Change Types
change_type_schema = StructType([
  StructField('change_type_id', IntegerType()),
  StructField('change_type', StringType())
  ])

@dlt.table(
  name = 'inventory_change_type',
  comment = 'data mapping change type id values to descriptive strings',
  table_properties={'quality':'silver'},
  spark_conf={'pipelines.trigger.interval':'24 hours'}
)
def inventory_change_type():
  return (
    spark
      .read
      .csv(
        config['change_types_filename'],
        header=True,
        schema=change_type_schema
        )
  )

# COMMAND ----------

# MAGIC %md ## Step 3: Stream Inventory Change Events
# MAGIC

# COMMAND ----------

# DBTITLE 1,Read Event Stream
@dlt.table(
  name = 'raw_inventory_change',
  comment= 'data representing raw (untransformed) inventory-relevant events originating from the POS',
  table_properties={'quality':'bronze'}
  )
def raw_inventory_change():
  return (
    spark
      .readStream
      .format('kafka')
      .option('subscribe', config['events_kafka_topic'])
      .option('kafka.bootstrap.servers', config['kafka_bootstrap_server'])
      .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
      .option('kafka.security.protocol', 'SASL_SSL')
      .option('kafka.sasl.jaas.config', config['kafka_sasl'])
      .option('kafka.request.timeout.ms', '60000')
      .option('kafka.session.timeout.ms', '60000')
      .option('failOnDataLoss', 'false')
      .option('startingOffsets', 'latest')
      .option('maxOffsetsPerTrigger', '100') # read 100 messages at a time
      .load()
  )

# COMMAND ----------

# DBTITLE 1,Convert Transaction to Structure Field & Extract Data Elements
# example of value field
# {
#    "date_time": "2021-01-01T01:03:55.000Z",
#    "trans_id": "60BE8AA9-7ECE-4337-9A62-799ADD5B2476",
#    "store_id": 0,
#    "change_type_id": 1,
#    "items": [
#       {
#          "item_id": 100893,
#          "quantity": -4
#       }
#    ]
# }



# schema of value field
value_schema = StructType([
  StructField('trans_id', StringType()),
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('change_type_id', IntegerType()),
  StructField('items', ArrayType(
    StructType([
      StructField('item_id', IntegerType()), 
      StructField('quantity', IntegerType())
      ])
    ))
  ])

# define inventory change data
@dlt.table(
  name = 'inventory_change',
  comment = 'data representing item-level inventory changes originating from the POS',
  table_properties = {'quality':'silver'}
)
def inventory_change():
  df = (
    dlt
      .read_stream('raw_inventory_change')
      .withColumn('body', f.expr('cast(value as string)')) # convert payload to string
      .withColumn('event', f.from_json('body', value_schema)) # parse json string in payload
      .select( # extract data from payload json
        f.col('event').alias('event'),
        f.col('event.trans_id').alias('trans_id'),
        f.col('event.store_id').alias('store_id'), 
        f.col('event.date_time').alias('date_time'), 
        f.col('event.change_type_id').alias('change_type_id'), 
        f.explode_outer('event.items').alias('item')     # explode items so that there is now one item per record
        )
      .withColumn('item_id', f.col('item.item_id'))
      .withColumn('quantity', f.col('item.quantity'))
      .drop('item')
      .withWatermark('date_time', '1 hour') # ignore any data more than 1 hour old flowing into deduplication
      .dropDuplicates(['trans_id','item_id'])  # drop duplicates 
    )
  return df

# COMMAND ----------

# MAGIC %md ## Step 4: Stream Inventory Snapshots

# COMMAND ----------

# DBTITLE 1,Read Change Data Stream
@dlt.table(
  name = 'raw_inventory_cdc_change',
  comment= 'data representing raw (untransformed) change data events originating from the inventory database',
  table_properties={'quality':'bronze'}
  )
def raw_inventory_cdc_change():
  return (
    spark
      .readStream
      .format('kafka')
      .option('subscribe', config['cdc_kafka_topic'])
      .option('kafka.bootstrap.servers', config['kafka_bootstrap_server'])
      .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
      .option('kafka.security.protocol', 'SASL_SSL')
      .option('kafka.sasl.jaas.config', config['kafka_sasl'])
      .option('kafka.request.timeout.ms', '60000')
      .option('kafka.session.timeout.ms', '60000')
      .option('failOnDataLoss', 'false')
      .option('startingOffsets', 'latest')
      .option('maxOffsetsPerTrigger', '100') # read 100 messages at a time
      .load()
  )

# COMMAND ----------

# DBTITLE 1,Apply change data change events
# example of cdc key
# {"item_id":100005,"store_id":0} 

# example of cdc value
# {
#    "before": null,
#    "after": {
#       "item_id": 100002,
#       "store_id": 0,
#       "employee_id": 1,
#       "date_time": 1609459200000000,
#       "quantity": 100
#    },
#    "source": {
#       "version": "2.1.1.Final",
#       "connector": "postgresql",
#       "name": "retail.updates",
#       "ts_ms": 1700586814840,
#       "snapshot": "false",
#       "db": "retail",
#       "sequence": "[\"91737821296\",\"91737821296\"]",
#       "schema": "public",
#       "table": "inventory",
#       "txId": 50257,
#       "lsn": 91737821296,
#       "xmin": null
#    },
#    "op": "c",
#    "ts_ms": 1700586815385,
#    "transaction": null
# }

key_schema_cdc = StructType([ \
  StructField("item_id", LongType(),True), \
  StructField("store_id", LongType(), True), \
  ])

value_schema_cdc = StructType([ \
  StructField("before", StructType(),True), \
  StructField("after", StructType([StructField("item_id", IntegerType()), StructField("store_id", IntegerType()), StructField("employee_id", IntegerType()), StructField("date_time", LongType()), StructField("quantity", IntegerType())]),True), \
  StructField("source", StructType(),True), \
  StructField("op", StringType(),True), \
  StructField("ts_ms", LongType(),True), \
  StructField("transaction", StringType(),True), \
  ])

reset = "true"

@dlt.table(comment="data extracted from debezium payload",
           table_properties={"pipelines.reset.allowed": reset},
           temporary=False)
def kafka_data_extracted_cdc():
  return(
      dlt.read_stream("raw_inventory_cdc_change")
      .withColumn("key_str", f.expr("cast(key as string)"))
      .withColumn("key_json", f.from_json("key_str", key_schema_cdc))
      .withColumn("value_str", f.expr("cast(value as string)"))
      .withColumn("value_json", f.from_json("value_str", value_schema_cdc))
      .select("key_json.item_id", "key_json.store_id", "value_json.ts_ms", "value_json.op" , "value_json.after.date_time", "value_json.after.quantity")
      .withColumn("date_time_ts", (f.col("date_time") / f.lit(1000000.)).cast('timestamp'))
  )

dlt.create_target_table('inventory_snapshot')

dlt.apply_changes(
    target = "inventory_snapshot",
    source = "kafka_data_extracted_cdc",
    keys = ["item_id","store_id"],
    sequence_by = f.col("ts_ms"),
    apply_as_deletes = f.expr("op = 'd'"),
    except_column_list = ["op", "ts_ms", "date_time"])
