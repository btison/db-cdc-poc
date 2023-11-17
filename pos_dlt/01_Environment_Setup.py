# Databricks notebook source
# DBTITLE 1,Initialize Config Settings
if 'config' not in locals():
  config = {}

# COMMAND ----------

# DBTITLE 1,Config Settings for DBFS Mount Point
config['dbfs_mount_name'] = f'/mnt/pos' 

# COMMAND ----------

mount_name = config['dbfs_mount_name']

# determine if not already mounted
for m in dbutils.fs.mounts():
  mount_exists = (m.mountPoint==config['dbfs_mount_name'])
  if mount_exists: break

# create mount if not exists
if not mount_exists:
  print('creating mount point {0}'.format(config['dbfs_mount_name']))

  access_key = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
  secret_key = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")
  encoded_secret_key = secret_key.replace("/", "%2F")
  aws_bucket_name = "databricks-pos-dlt"

  # create mount
  dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"{mount_name}")

display(dbutils.fs.ls(f"{mount_name}"))



# COMMAND ----------

# DBTITLE 1,Config Settings for Data Files
# change event data files
config['inventory_change_store001_filename'] = config['dbfs_mount_name'] + '/inventory_change_store001_1000.txt'
config['inventory_change_online_filename'] = config['dbfs_mount_name'] + '/inventory_change_online_1000.txt'

# snapshot data files
config['inventory_snapshot_store001_filename'] = config['dbfs_mount_name'] + '/inventory_snapshot_store001_1000.txt'
config['inventory_snapshot_online_filename'] = config['dbfs_mount_name'] + '/inventory_snapshot_online_1000.txt'

# static data files
config['stores_filename'] = config['dbfs_mount_name'] + '/store.txt'
config['items_filename'] = config['dbfs_mount_name'] + '/item_1000.txt'
config['change_types_filename'] = config['dbfs_mount_name'] + '/inventory_change_type.txt'

# COMMAND ----------

# DBTITLE 1,Config Settings for DLT Data
config['dlt_pipeline'] = config['dbfs_mount_name'] + '/dlt_pipeline'

# COMMAND ----------

# DBTITLE 1,Identify Database for Data Objects and initialize it
database_name = f'pos_dlt'
config['database'] = database_name
