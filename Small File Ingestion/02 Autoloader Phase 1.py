# Databricks notebook source
# MAGIC %sql
# MAGIC USE demo.file_ingestion;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS small_files;

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -Rf /Volumes/demo/file_ingestion/checkpoints/small_files

# COMMAND ----------

import time
from pyspark.sql.functions import current_timestamp

file_path = "/Volumes/demo/file_ingestion/small_files"
checkpoint_path = "/Volumes/demo/file_ingestion/checkpoints/small_files"
table_name = "small_files"

start = time.time()

stream_query_handler = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))
stream_query_handler.awaitTermination()

end = time.time()

# COMMAND ----------

import math
minutes = math.floor((end - start)/60)
seconds = (end - start) - (minutes * 60)
print(f"Time elapsed to ingest 10k is {round(minutes,2)} minutes and {round(seconds,1)} seconds")
