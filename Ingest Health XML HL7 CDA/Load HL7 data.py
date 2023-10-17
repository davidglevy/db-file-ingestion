# Databricks notebook source
# MAGIC %pip install xmlschema

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo.file_ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME ccda_examples

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME ccda_schema;

# COMMAND ----------

import os
cwd = os.getcwd()
print(cwd)

# COMMAND ----------

volume_path = "/Volumes/demo/file_ingestion/ccda_examples/CCD.xml"
dbutils.fs.cp(f"file://{cwd}/CCD.xml", volume_path, True)

# COMMAND ----------

df = spark.read.format("binaryFile").load(volume_path)
df.write.mode("overwrite").saveAsTable("xml_hl7_binary")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE xml_hl7_binary;

# COMMAND ----------

# MAGIC %sh
# MAGIC find /tmp/processable

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -Rf /tmp/xsd
# MAGIC rm -Rf /tmp/processable

# COMMAND ----------

dbutils.fs.cp("/Volumes/demo/file_ingestion/ccda_schema/CDA.xsd", "file:///tmp/xsd/cda/CDA.xsd")
dbutils.fs.cp("/Volumes/demo/file_ingestion/ccda_schema/POCD_MT000040.xsd", "file:///tmp/xsd/cda/POCD_MT000040.xsd")
dbutils.fs.cp("/Volumes/demo/file_ingestion/ccda_schema/NarrativeBlock.xsd", "file:///tmp/processable/coreschemas/NarrativeBlock.xsd")
dbutils.fs.cp("/Volumes/demo/file_ingestion/ccda_schema/datatypes-base.xsd", "file:///tmp/processable/coreschemas/datatypes-base.xsd")
dbutils.fs.cp("/Volumes/demo/file_ingestion/ccda_schema/datatypes.xsd", "file:///tmp/processable/coreschemas/datatypes.xsd")
dbutils.fs.cp("/Volumes/demo/file_ingestion/ccda_schema/voc.xsd", "file:///tmp/processable/coreschemas/voc.xsd")


# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /git-clone
# MAGIC cd /git-clone
# MAGIC git clone https://github.com/HL7/CDA-core-2.0.git

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /git-clone/CDA-core-2.0/schema/extensions/SDTC/infrastructure/cda
# MAGIC ls

# COMMAND ----------

from xmlschema import XMLSchema, etree_tostring

# load a XSD schema file
schema = XMLSchema("/git-clone/CDA-core-2.0/schema/extensions/SDTC/infrastructure/cda/CDA_SDTC.xsd")

# validate against the schema
schema.validate("/Volumes/demo/file_ingestion/ccda_examples/CCD.xml")

# or
#schema.is_valid("your_file.xml")

# decode a file
data = schema.decode("/Volumes/demo/file_ingestion/ccda_examples/CCD.xml")

# encode to string
s = etree_tostring(schema.encode(data))

# COMMAND ----------

print(s)

# COMMAND ----------

print(data['title']['$'])

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.spark.xml.util.XSDToSchema
# MAGIC import java.nio.file.Paths
# MAGIC
# MAGIC //val schemaParsed = XSDToSchema.read(Paths.get("/Volumes/demo/file_ingestion/ccda_schema/CDA.xsd"))
# MAGIC val schemaParsed = XSDToSchema.read(Paths.get("/tmp/xsd/cda/CDA.xsd"))
# MAGIC print(schemaParsed)

# COMMAND ----------

with open('/Volumes/demo/file_ingestion/ccda_schema/CDA.xsd', 'r') as file:
    text = file.read()
print(text[0:200])

# COMMAND ----------

spark.sql(f"SELECT *, from_xml(content, '{text}') FROM xml_hl7_binary")

# COMMAND ----------


#df = spark.read.format("xml").option("inferSchema", True).option("multiline", True).load(volume_path)
df = spark.read.format("xml").option("rowTag", "ClinicalDocument").option("inferSchema", True).load(volume_path)

# COMMAND ----------

display(df)
