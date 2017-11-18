# Eloqua integration with Google Cloud Platform
Example integration of Eloqua and Google Cloud Platform (GCP)

## Goals

*Create ETL pipeline for loading Email Activity data in to GCP*
- Run export of activity data using the pyeloqua package
  + Create BigQuery table schema
  + Use Dataflow to export raw JSON data and load in to BigQuery
