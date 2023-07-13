# File Ingestion Benchmarks

This repository runs a suite of file ingestion tests. We cover the following scenarios:

1. Ingest 10,000 small CSV files (100KB) with the Autoloader
    1. Using directory listing
    2. Using storage notification
    3. Test should show performance with 10,000 incremental and 10,000 already processed
2. Ingest 20GB CSV
3. Ingest 5GB XML
4. XML schema detection/processing

## Prerequisites

* **Unity Catalog** - We use Unity Catalog Volumes to store the raw data
* **Enable System Tables** - We are going to montior usage with System Tables in Azure Databricks: https://learn.microsoft.com/en-us/azure/databricks/administration-guide/system-tables/
* The generation of synthetic data requires Faker

## Getting Started
1. First import this Github repository to your Databricks Repo
2. By default, this script will run against the catalog "demo" and schema "file_ingestion"
3. If you want to change the default catalog/schema:
    1. Define a file BENCHMARK_PROPERTIES.config
    2. Define the properties:
        * CATALOG
        * SCHEMA


