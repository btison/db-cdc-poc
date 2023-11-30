# Databricks notebook source
# DBTITLE 1,Install required libraries
!pip install psycopg2
!pip install confluent-kafka

# COMMAND ----------

dbutils.widgets.text("mode", "prod")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import *
import pyspark.sql.functions as f

import datetime, time

# COMMAND ----------

# DBTITLE 1,Notebook Configuration
# MAGIC %run "./01_Environment_Setup"

# COMMAND ----------

# DBTITLE 1,Reset the Target Database
_ = spark.sql("DROP DATABASE IF EXISTS {0} CASCADE".format(config['database']))
_ = spark.sql("CREATE DATABASE IF NOT EXISTS {0}".format(config['database']))

# COMMAND ----------

# DBTITLE 1,Reset the DLT Environment
dbutils.fs.rm(config['dlt_pipeline'],True)

# COMMAND ----------

# DBTITLE 1,Combine & Reformat Inventory Change Records
inventory_change_schema = StructType([
  StructField('trans_id', StringType()),  # transaction event ID
  StructField('item_id', IntegerType()),  
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('quantity', IntegerType()),
  StructField('change_type_id', IntegerType())
  ])

# inventory change record data files (one from each store)
inventory_change_files = [
  config['inventory_change_store001_filename'],
  config['inventory_change_online_filename']
  ]

# read inventory change records and group items associated with each transaction so that one output record represents one complete transaction
inventory_change = (
  spark
    .read
    .csv(
      inventory_change_files, 
      header=True, 
      schema=inventory_change_schema, 
      timestampFormat='yyyy-MM-dd HH:mm:ss'
      )
    .withColumn('trans_id', f.expr('substring(trans_id, 2, length(trans_id)-2)')) # remove surrounding curly braces from trans id
    .withColumn('item', f.struct('item_id', 'quantity')) # combine items and quantities into structures from which we can build a list
    .groupBy('date_time','trans_id')
      .agg(
        f.first('store_id').alias('store_id'),
        f.first('change_type_id').alias('change_type_id'),
        f.collect_list('item').alias('items')  # organize item info as a list
        )
    .orderBy('date_time','trans_id')
    .toJSON()
    .collect()
  )

# print a single transaction record to illustrate data structure
eval(inventory_change[0])

# COMMAND ----------

# DBTITLE 1,Access Inventory Snapshots
inventory_snapshot_schema = StructType([
  StructField('item_id', IntegerType()),
  StructField('employee_id', IntegerType()),
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('quantity', IntegerType())
  ])

# inventory snapshot files
inventory_snapshot_files = [ 
  config['inventory_snapshot_store001_filename'],
  config['inventory_snapshot_online_filename']
  ]

# read inventory snapshot data
inventory_snapshots = (
  spark
    .read
    .csv(
      inventory_snapshot_files, 
      header=True, 
      timestampFormat='yyyy-MM-dd HH:mm:ss', 
      schema=inventory_snapshot_schema
      )
  )

display(inventory_snapshots)

# COMMAND ----------

# DBTITLE 1,Assemble Set of Snapshot DateTimes by Store
inventory_snapshot_times = (
  inventory_snapshots
    .select('date_time','store_id')
    .distinct()
    .orderBy('date_time')  # sorting of list is essential for logic below
  ).collect()

# display snapshot times
inventory_snapshot_times

# COMMAND ----------

# DBTITLE 1,Insert/Update snapshot records in PostgreSQL
import numpy as np
import psycopg2
from psycopg2.extensions import register_adapter, AsIs

def insert_snapshot(snapshot_df):

  register_adapter(np.int32, AsIs)

  conn = None
  try:
    # connect to the PostgreSQL server
    # print('Connecting to the PostgreSQL database...')
    postgresql_host = dbutils.secrets.get(scope = "postgresql", key = "host")
    postgresql_user = dbutils.secrets.get(scope = "postgresql", key = "username")
    postgresql_password = dbutils.secrets.get(scope = "postgresql", key = "password")

    conn = psycopg2.connect(host=postgresql_host, database="pos", user=postgresql_user, password=postgresql_password)
		
    # create a cursor
    cur = conn.cursor()
        
    sql = """INSERT INTO public.inventory(item_id, store_id, employee_id, date_time, quantity) 
             VALUES (%s,%s,%s,%s,%s)
             ON CONFLICT(item_id, store_id)
             DO UPDATE SET employee_id = EXCLUDED.employee_id, date_time = EXCLUDED.date_time, quantity = EXCLUDED.quantity;"""

    # loop over dataframe and insert/update inventory
    for ind in snapshot_df.index:
      # print("Inserting row for item {0}".format(snapshot_df['item_id'][ind]))
      record_to_insert = (snapshot_df['item_id'][ind], snapshot_df['store_id'][ind], snapshot_df['employee_id'][ind], "'" + snapshot_df['date_time'][ind] + "'", snapshot_df['quantity'][ind])
      cur.execute(sql, record_to_insert)
      conn.commit()
       
    # close the communication with the PostgreSQL
    cur.close()
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
    #   print('Database connection closed.')

# COMMAND ----------

# DBTITLE 1,Send inventory change events to Kafka
from confluent_kafka import Producer

def createProducer():
  conf = {'bootstrap.servers': dbutils.secrets.get(scope = "kafka", key = "bootstrap-server"),
          'security.protocol': "SASL_SSL",
          'sasl.username': dbutils.secrets.get(scope = "kafka", key = "username"), 
          'sasl.password': dbutils.secrets.get(scope = "kafka", key = "password"),
          'ssl.endpoint.identification.algorithm': "https",
          'sasl.mechanism': "SCRAM-SHA-512"
          }

  # Create Producer instance
  p = Producer(**conf)

  return p

def delivery_callback(err, msg):
  if err:
    sys.stderr.write('Message failed delivery: {err}')

def sendChangeEvent(producer, topic, event):
  try:
    # Produce line (without newline)
    producer.produce(topic, event, callback=delivery_callback)

  except BufferError:
      print(f'%% Local producer queue is full {len(producer)} messages awaiting delivery: try again\n')


# COMMAND ----------

# DBTITLE 1,Load data
event_speed_factor = 250 # Send records to iot hub at <event_speed_factor> X real-time speed
last_dt = None

producer = createProducer()
topic = "inventory.event"

for event in inventory_change:
  # extract datetime from transaction document
  d = eval(event) # evaluate json as a dictionary
  dt = datetime.datetime.strptime( d['date_time'], '%Y-%m-%dT%H:%M:%S.000Z')
  
  # inventory snapshot transmission
  # -----------------------------------------------------------------------
  snapshot_start = time.time()
  
  inventory_snapshot_times_for_loop = inventory_snapshot_times # copy snapshot times list as this may be modified in loop
  for snapshot_dt, store_id in inventory_snapshot_times_for_loop: # for each snapshot
    
    # if event date time is before next snapshot date time
    if dt < snapshot_dt: # (snapshot times are ordered by date)
      break              #   nothing to transmit
      
    else: # event date time exceeds a snapshot datetime
      
      # extract snapshot data for this dt
      snapshot_pd = (
        inventory_snapshots
          .filter(f.expr("store_id={0} AND date_time='{1}'".format(store_id, snapshot_dt)))
          .withColumn('date_time', f.expr("date_format(date_time, 'yyyy-MM-dd HH:mm:ss')")) # force timestamp conversion to include 
          .toPandas()  
           )
        
      # transmit to PostgreSQL
      # print('Inserting inventory snapshot for {0}'.format(snapshot_dt.strftime('%Y-%m-%d %H:%M:%S')))
      insert_snapshot(snapshot_pd)
      
      # remove snapshot date from inventory_snapshot_times
      inventory_snapshot_times.pop(0)
      print('Loaded inventory snapshot for {0}'.format(snapshot_dt.strftime('%Y-%m-%d %H:%M:%S')))
      
  snapshot_seconds = time.time() - snapshot_start
  # -----------------------------------------------------------------------
  
  # inventory change event transmission
  # -----------------------------------------------------------------------
  # calculate delay (in seconds) between this event and prior event (account for snapshot)
  if last_dt is None: last_dt = dt
  delay = (dt - last_dt).seconds - snapshot_seconds
  delay = int(delay/event_speed_factor) # adjust delay by event speed factor
  if delay < 0: delay = 0
  
  # sleep for delay duration
  # print('Sleep for {0} seconds'.format(delay))
  time.sleep(delay)
  
  # send transaction document
  sendChangeEvent(producer, topic, event)
    
  # -----------------------------------------------------------------------
  
  # set last_dt for next cycle
  last_dt = dt
