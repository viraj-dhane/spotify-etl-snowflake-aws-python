# Spotify ETL Pipeline using Snowflake, AWS, and Python

## Project Overview
This project showcases the implementation of an automated ETL (Extract, Transform, Load) pipeline that ingests music-related data from the Spotify API and loads it into a Snowflake data warehouse for downstream analytics. The solution leverages a serverless architecture using AWS Lambda, S3, CloudWatch, and Snowflake’s Snowpipe, with Python and SQL as development language.

## Data Architecture
![data-architecture](https://github.com/user-attachments/assets/21de72e8-1aa2-4ae8-b960-b7b57dc08f88)

## Data Extraction
  - Developed Python script to extract data from Spotify API
  - Deployed data extraction logic as AWS Lambda function to enable serverless execution
  - Scheduled the function using AWS CloudWatch Events to run at defined intervals
  - Stored the extracted raw JSON data in AWS S3 bucket for staging and archival purposes

## Data Transformation
  - Implemented data transformation logic using Python within another AWS Lambda function
  - Configured S3 event notifications to automatically trigger the transformation function upon the arrival of new raw data
  - Saved the transformed, clean data in separate “transformed_data” S3 bucket folder for downstream consumption

## Data Loading
  - Set up Snowflake external stages and file formats to define how Snowflake should interpret data from S3
  - Configured Snowpipe to automatically detect and load new files from the processed S3 bucket into Snowflake tables in near real-time
  - Wrote SQL script/queries to create:
      - Database and schema
      - Data tables
      - Storage integrations with IAM role
      - Snowpipe definitions for automated ingestion
