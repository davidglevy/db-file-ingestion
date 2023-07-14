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
# MAGIC CREATE VOLUME IF NOT EXISTS large_xml;
# MAGIC CREATE VOLUME IF NOT EXISTS checkpoints;

# COMMAND ----------

from faker import Faker
from xml.sax.saxutils import escape

generator = Faker()


with open(f"/tmp/large.xml", 'w', newline='') as myfile:
    myfile.write("<people>\n")

    # 5550 creates a 1MB file, so we multiple by 1000 to get a GB size file then by 5 to get 5GB
    for x in range(5611 * 1000 * 5):

        person_id = x
        name = escape(generator.name())
        address = escape(generator.address()).replace("\n", "&#10;")
        dob = generator.date_of_birth()
        gender = generator.passport_gender()
        xml_fragment = f"\t<person><id>{person_id}</id><name>{name}</name><address>{address}</address><date-of-birth>{dob}</date-of-birth><gender>{gender}</gender></person>\n"
        myfile.write(xml_fragment)
    myfile.write("</people>\n")



# COMMAND ----------

# MAGIC %sh
# MAGIC du -ksh /tmp/large.xml

# COMMAND ----------

cp /tmp/large.xml /Volumes/demo/file_ingestion/large_xml/large.xml
