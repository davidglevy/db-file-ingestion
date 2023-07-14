# Databricks notebook source
# MAGIC %pip install mack

# COMMAND ----------

spark.sql("USE demo.file_ingestion")

# COMMAND ----------

schema = "id bigint, name string, address string, `date-of-birth` string, gender string"


df = spark.read.format('com.databricks.spark.xml').option("rowTag", "person").option("rootTag", "people").load("/Volumes/demo/file_ingestion/large_xml/large.xml", schema=schema)
display(df)

# COMMAND ----------

df.repartition(16).write.mode("overwrite").saveAsTable("large_from_xml")

# COMMAND ----------

import mack
from delta import DeltaTable

csv_tb = DeltaTable.forName(spark, "large_from_xml")

size_gb = mack.delta_file_sizes(csv_tb)['size_in_bytes'] / (1024 * 1024 * 1024)
reduction_pc = (5 - size_gb) / 5
print(f"Final size is {round(size_gb, 2)}GB with file reduced by {round(reduction_pc * 100, 1)}%")

# COMMAND ----------

spark.sql("ANALYZE TABLE large_from_xml COMPUTE STATISTICS")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM large_from_xml;
