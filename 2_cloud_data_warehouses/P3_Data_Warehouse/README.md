# Project: Data Modeling with Postgres

## Project Description

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal of this project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## How to run the project

1.To run this project locally, a configuration file dwh.cfg needs to be created and placed in the root folder with required information.

```
[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large
DWH_IAM_ROLE_NAME=
DWH_CLUSTER_IDENTIFIER=
DWH_DB=
DWH_DB_USER=
DWH_DB_PASSWORD=
DWH_PORT=5439

[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=5439

[AWS]
KEY=
SECRET=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[IAM_ROLE]
ARN=
```
2.Run create_cluster.py to create necessary infrastructure for the project
3.Add the IAM role ARN created in the previous step to the configuration file
4.Run create_table.py to create dimension, fact and staging tables
5.Run etl.py to copy data from S3 to Redshift staging and dimensional tables and insert data into fact table


## Project files

1. **sql_queries.py** Consists of all the sql queries - DROP, CREATE, INSERT and COPY commands
2. **create_tables.py** Drops and creates the database and tables.
3. **create_cluster.py** Creates an redshift cluster with appropriate sizing and IAM role.  
4. **etl.py** Reads and processes all the files from S3 bucket, inserts and copy data into respective tables

## Database Schema

Star Schema was used for this project.Star schemas are organized into fact and dimension tables in which facts are measurable data about an event while dimensions are attrributes to describe the fact data.
We have used staging tables which are temporary tables that holds all of the data that will be used to make changes to the target table, including both updates and inserts.

#### Staging Tables
- **staging_events**
- **staging_songs**

#### Fact Table
- **songplays** - Records from event data which simulate app activity logs (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location,                   user_agent)

#### Dimension Tables
- **users** - Users of the app (user_id, first_name, last_name, gender, level)
- **songs** - songs in the song database (song_id, title, artist_id, year, duration)
- **artists** - Artists for the songs in the song database (artist_id, name, location, lattitude, longitude)
- **time** - Timestamp of event data broken into specific units (start_time, hour, day, week, month, year, weekday)

## Detailed description of ETL pipeline

1. First the database schema is designed with appropriate Fact and Dimension tables.
2. Created create table statements for dimension, fact and staging tables and copy command to ingest data into redshift from S3 in sql_queries.py
3. Created create_tables.py script to connect to database and run queries and copy commands from sql_queries.py
4. Created scripts using python boto3 sdk to create Redshift cluster with appropriate IAM roles, security group and ingress rule.

