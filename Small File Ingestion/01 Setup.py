# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO Move this into a common Python module
# MAGIC -- TODO Read the catalog and schema from 
# MAGIC CREATE CATALOG IF NOT EXISTS demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS demo.file_ingestion;
# MAGIC USE demo.file_ingestion;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS small_files;
# MAGIC CREATE VOLUME IF NOT EXISTS checkpoints;

# COMMAND ----------

from faker import Faker
import csv
import json

generator = Faker()
generator.seed_instance(100)

for x in range(10000):
    print(f"Writing file {str(x).rjust(6, ' ')}")
    entry = {}
    entry['id'] = x
    entry['name'] = generator.name()
    entry['address'] = generator.address()
    entry['date-of-birth'] = generator.date_of_birth().strftime('y-m-d')
    entry['gender'] = generator.passport_gender()

    with open(f"/Volumes/demo/file_ingestion/small_files/small_{str(x).rjust(6, '0')}.csv", 'w') as myfile:
        json.dump(entry, myfile, sort_keys=False)
