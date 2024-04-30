# Amazon ETL Pipeline

## Table of Contents

- [ETL Pipeline Overview](#etl-pipeline-overview)
  - [Extract](#extract)
  - [Transform](#transform)
  - [Load](#load)
  - [DAG Script for Apache Airflow](#dag-script-for-apache-airflow)
- [Opportunities for Improvement](#opportunities-for-improvement)

## ETL Pipeline Overview

This ETL pipeline is an expansion of my previous project [Amazon Bestseller PC Gaming Mice Analysis](https://github.com/raufh10/Amazon_Gaming_Mice_Data_Analysis), which included an ETL component. I have now developed it into a full ETL pipeline. I'm using Python for extracting, transforming, and loading the data, specifically utilizing BeautifulSoup (bs4) and Playwright for web scraping. For pipeline orchestration, I use Apache Airflow, and for the database, I use NoSQL database MongoDB. This project focuses solely on building the pipeline, not on constructing a data warehouse.

Here are picture of pipeline architecture:

![ETL Pipeline Architecture](images/etl_pipeline_architecture.png)

The ETL pipeline consists of the following steps:

### Extract

This Python script automates the extraction of products data from Amazon. It navigates to specific best seller page URLs  and extracts details such as asin, price, and its rank. The extracted data is temporarily stored for further processing. The script uses `BeautifulSoup` for HTML parsing and `playwright.sync_api` for browser automation to efficiently handle web data extraction.

### Transform

This Python script is designed to transform data related to products data from Amazon. Here's what it does:

- *Clean Float Data*: This function loads product data from a JSON file, cleans and converts price and rating fields into float values, and then saves the updated data back to the same file.
- *Clean Integer Data*: This function loads product data from a JSON file, cleans and converts offers (if applicable), rank, and rating volume fields into integer values, and then saves the updated data back to the same file.

The script focuses on refining and enhancing data quality for better usability in data analysis or further data processing tasks. It uses JSON for data handling and incorporates custom functions to clean and organize the data effectively.

### Load

This Python script is tailored for loading job data into a NoSQL MongoDB database and managing post-load cleanup:

- *Load Data*: Connects to MongoDB using credentials, inserted data into the database.
- *Cleanup*: Deletes the JSON file after loading data into the database.

The script utilizes `pymongo` for database operations.

### DAG Script for Apache Airflow

You can also find a DAG script for Apache Airflow in the `dags` directory. The script is designed to automate the ETL pipeline by scheduling and orchestrating the execution of the individual scripts. The DAG script defines the sequence of tasks and their dependencies, ensuring the proper execution of the ETL pipeline.

## Opportunities for Improvement

There are several opportunities for improvement in the ETL pipeline:

1. **Parallel Pipeline for Monitoring**: Implement a parallel pipeline that runs concurrently with the main pipeline to monitor its performance and measure processing metrics, ensuring real-time tracking and optimization.
2. **Error Handling**: Enhance error handling mechanisms to catch and respond to exceptions more effectively, reducing downtime and ensuring the pipeline's reliability.
