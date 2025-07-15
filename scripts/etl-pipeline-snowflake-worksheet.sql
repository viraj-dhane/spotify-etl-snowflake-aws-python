-- Database
CREATE DATABASE spotify_db;

-- Schema
CREATE SCHEMA spotify_etl;

-- File format
CREATE OR REPLACE FILE FORMAT SPOTIFY_DB.SPOTIFY_ETL.csv_file_format
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL','null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::543182730011:role/snowflake-s3-connection'
    STORAGE_ALLOWED_LOCATIONS = ('s3://data-engg-snowflake-data-warehousing')
      COMMENT = 'Creating connection to snowflake and S3 bucket'

-- Tables to store transformed data
CREATE OR REPLACE TABLE SPOTIFY_DB.SPOTIFY_ETL.songs (
    song_id VARCHAR,
    song_name VARCHAR,
    duration_ms INT,
    url VARCHAR,
    popularity INT,
    song_added TIMESTAMP_NTZ,
    album_id VARCHAR,
    artist_id VARCHAR
);

CREATE OR REPLACE TABLE SPOTIFY_DB.SPOTIFY_ETL.artist (
    artist_id VARCHAR,
    artist_name VARCHAR,
    external_url VARCHAR
);

CREATE OR REPLACE TABLE SPOTIFY_DB.SPOTIFY_ETL.album (
    album_id VARCHAR,
    name VARCHAR,      
    release_date DATE,
    total_tracks INT,
    url VARCHAR
);

-- Stage objects with storage integration & file format object
CREATE OR REPLACE STAGE SPOTIFY_DB.SPOTIFY_ETL.album_data
    URL = 's3://data-engg-snowflake-data-warehousing/transformed_data/album_data/'
    STORAGE_INTEGRATION = s3_init
    file_format = SPOTIFY_DB.SPOTIFY_ETL.csv_file_format;

CREATE OR REPLACE STAGE SPOTIFY_DB.SP_ETL.artist_data
    URL = 's3://data-engg-snowflake-data-warehousing/transformed_data/artist_data/'
    STORAGE_INTEGRATION = s3_init
    file_format = SPOTIFY_DB.SPOTIFY_ETL.csv_file_format;

CREATE OR REPLACE STAGE SPOTIFY_DB.SP_ETL.songs_data
    URL = 's3://data-engg-snowflake-data-warehousing/transformed_data/transformed_data/songs_data/'
    STORAGE_INTEGRATION = s3_init
    file_format = SPOTIFY_DB.SPOTFY_ETL.csv_file_format;
    

-- Pipes -- 3 pipes for 3 tables - album, artist, and songs

CREATE OR REPLACE PIPE SPOTIFY_DB.SP_ETL.album
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY_DB.SPOTIFY_ETL.album
FROM @SPOTIFY_DB.SPOTIFY_ETL.album_data;

DESC pipe SPOTIFY_DB.SPOTIFY_ETL.album;


CREATE OR REPLACE PIPE SPOTIFY_DB.SP_ETL.artist
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY_DB.SPOTIFY_ETL.artist
FROM @SPOTIFY_DB.SPOTIFY_ETL.artist_data;

DESC pipe SPOTIFY_DB.SPOTIFY_ETL.artist;


CREATE OR REPLACE PIPE SPOTIFY_DB.SP_ETL.songs
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY_DB.SPOTIFY_ETL.songs
FROM @SPOTIFY_DB.SPOTIFY_ETL.songs_data;

DESC pipe SPOTIFY_DB.SPOTIFY_ETL.songs;
  

SELECT * FROM SPOTIFY_DB.SPOTIFY_ETL.album;
SELECT * FROM SPOTIFY_DB.SPOTIFY_ETL.artist;
SELECT * FROM SPOTIFY_DB.SPOTIFY_ETL.songs;
