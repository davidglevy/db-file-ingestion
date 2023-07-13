# Databricks notebook source
# MAGIC %pip install mack

# COMMAND ----------

spark.sql("USE demo.file_ingestion")

# COMMAND ----------

schema = "id bigint, name string, address string, date_of_birth string"
df = spark.read.csv("/Volumes/demo/file_ingestion/large_csv/large.csv", multiLine=True, schema=schema)
display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("large_from_csv")

# COMMAND ----------

import mack
from delta import DeltaTable

csv_tb = DeltaTable.forName(spark, "large_from_csv")

size_gb = mack.delta_file_sizes(csv_tb)['size_in_bytes'] / (1024 * 1024 * 1024)
reduction_pc = (20 - size_gb) / 20
print(f"Final size is {round(size_gb, 2)}GB with file reduced by {round(reduction_pc, 2)}%")
