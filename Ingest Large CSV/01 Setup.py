# Databricks notebook source
# MAGIC %md
# MAGIC # Setup
# MAGIC We now run common setup, creating the catalog and schema if necessary and creating our volume. We then generate the synthetic data and store it in the volume.
# MAGIC
# MAGIC We also remove any checkpoints if they exist. We used a single node D14_v2 VM to generate the 20GB file.

# COMMAND ----------

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
# MAGIC CREATE VOLUME IF NOT EXISTS large_csv;
# MAGIC CREATE VOLUME IF NOT EXISTS checkpoints;

# COMMAND ----------

from faker import Faker
import csv

generator = Faker()

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -f /tmp/large*.csv

# COMMAND ----------


generator.seed_instance(100)

print("Generating the list")

keys = ['id', 'name', 'address', 'age', 'gender']

for y in range(1,21):

    print(f"We're generating fragment {str(y).rjust(2, '0')}")
    entries = []
    # 2250 is 1MB CSV file.
#    for x in range(2250 * 1000):
    for x in range(13000 * 973):
        entry = {}
        entry['id'] = x
        entry['name'] = generator.name()
        entry['address'] = generator.address()
        entry['age'] = generator.date_of_birth()
        entry['gender'] = generator.passport_gender()
        entries.append(entry)

        if x % 10000 == 0 and x > 0:
            millionth = int(x / 10000)
            print(f"Writing {millionth}0k entry")
            with open(f"/tmp/large_fragment_{str(y).rjust(2, '0')}.csv", 'a', newline='') as myfile:
            #with open("/Volumes/demo/file_ingestion/large_csv/test_file.csv", 'w', newline='') as myfile:
                wr = csv.DictWriter(myfile, keys)
                wr.writerows(entries)
            entries = []

# COMMAND ----------

with open(f"/tmp/large.csv", 'a') as final_file:

    for x in range(1,21):
        with open(f"/tmp/large_fragment_{str(x).rjust(2, '0')}.csv") as f:
            print(f"About to start appending to large.csv for {x}")
            for line in f:
                final_file.write(line)

# COMMAND ----------

# MAGIC %sh
# MAGIC du -ksh /tmp/large_fragment_01.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC du -ksh /tmp/large.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /tmp/large.csv /Volumes/demo/file_ingestion/large_csv/large.csv

# COMMAND ----------

schema = ",".join([f"{str(x)} string" for x in keys])

df = spark.read.csv("/Volumes/demo/file_ingestion/large_csv/large.csv", schema, multiLine=True)
display(df)
