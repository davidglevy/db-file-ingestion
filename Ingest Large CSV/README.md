# Large CSV Ingestion Test

![CSV Lineage](https://github.com/davidglevy/db-file-ingestion/blob/main/Ingest%20Large%20CSV/csv-lineage.png?raw=true)

## Methodology
1. Generate the 20GB large CSV in our large_csv Volume
2. Run ingestion, converting the file to Delta
3. Show size difference before/after conversion.
