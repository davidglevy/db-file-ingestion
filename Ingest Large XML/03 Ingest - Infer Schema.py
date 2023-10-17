# Databricks notebook source
# MAGIC %md
# MAGIC # Write a 5GB XML File with inferred schema

# COMMAND ----------

spark.sql("USE demo.file_ingestion")

# COMMAND ----------

path = "/Volumes/demo/file_ingestion/large_xml/large.xml"
df = spark.read.format("xml").option("rowTag", "person").option("inferSchema", True).load(path)


# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("inferred_from_xml")

# COMMAND ----------

# MAGIC %sh
# MAGIC du -ksh "/Volumes/demo/file_ingestion/large_xml/large.xml"
