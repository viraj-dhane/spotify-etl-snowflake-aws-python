-- Database to manage objects - stages, file formats, storage integration, snowpipe etc.
CREATE OR REPLACE DATABASE MANAGE_DB;

-- Schema to manage snowpipes
CREATE OR REPLACE SCHEMA MANAGE_DB.snowpipes;

-- Schema to manage external stages
CREATE OR REPLACE SCHEMA MANAGE_DB.external_stages;

-- Schema to manage file formats
CREATE OR REPLACE SCHEMA MANAGE_DB.file_formats;

-- Create CSV file format object
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL','null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::543182730011:role/snowflake-s3-connection'
  STORAGE_ALLOWED_LOCATIONS = ('s3://data-engg-snowflake-data-warehousing')
    COMMENT = 'Creating connection to snowflake data warehousing S3 bucket'

DESC INTEGRATION s3_init

/*
To integrate(to create relationship between) AWS S3 and Snowflake (Storage Integration)
1. Create IAM role with type AWS account + requires external ID
2. To initiate use external ID '00000' as dummy ID > Provide S3 Full Access to the role > Role Name > Create
3. Create storage integration in Snowflake with STORAGE_AWS_ROLE_ARN from created role.
4. Edit Trust Policy in AWS S3
5. Get IAM_USER_ARN and EXTRNAL_ID from properties of created storage integration
6. Edit Trust Policy in AWS S3 with IAM_USER_ARN and EXTRNAL_ID from step 5
*/

-- Create Snowpipe
/* Setting up Snowpipe
1. Create Stage - to have the connection
2. Test COPY COMMAND
3. Create Pipe - create pipe as object with COPY COMMAND
4. S3 Notification - to trigger snowpipe
*/

CREATE OR REPLACE DATABASE TEST_DB;

-- Create table
CREATE OR REPLACE TABLE TEST_DB.PUBLIC.employees (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
  )

-- Create stage object with integration object & file format object
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.employee_data_snowpipe
    URL = 's3://data-engg-snowflake-data-warehousing/employee-data-snowpipe/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = MANAGE_DB.file_formats.csv_file_format

LIST @MANAGE_DB.external_stages.employee_data_snowpipe

-- Create snowpipe
CREATE OR REPLACE PIPE MANAGE_DB.snowpipes.employees_pipe
AUTO_INGEST = TRUE
AS
COPY INTO TEST_DB.PUBLIC.employees
FROM @MANAGE_DB.external_stages.employee_data_snowpipe

-- Description of pipe to get the notification_channel and copy it for S3 event (to set notification event in S3 bucket)
/*
S3 bucket > Properties > Create Event Notification > ... > SQS queue > Enter SQS queue ARN
*/
DESC pipe MANAGE_DB.snowpipes.employees_pipe

SELECT * FROM TEST_DB.PUBLIC.employees
