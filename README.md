Motor Insurance ETL Pipeline on GCP (Personal Project, 2025) 

● Developed an end-to-end ETL pipeline using Apache Airflow and Python to automate insurance 
data ingestion and transformation, processing approximately 100K rows of data. 

● Extracted CSV data from Google Cloud Storage (GCS) using GCSToLocalFilesystemOperator. 

● Cleaned data using Pandas, handling missing values, standardizing date columns, and resolving 
logical contradictions across fields (e.g., contract start/end and lapse status). 

● Validated and logged each cleaning and transformation step for maintainability and debugging. 

● Saved cleaned datasets as Parquet files and loaded into Google BigQuery via 
GCSToBigQueryOperator. 

● Demonstrated full pipeline orchestration and modular task handling using Airflow's TaskFlow 
API.

Medium:Project documentation available on [Medium](https://medium.com/@wittawatsuwannarak/building-a-robust-motor-insurance-etl-pipeline-on-gcp-with-apache-airflow-python-b95cd8d81adb)
