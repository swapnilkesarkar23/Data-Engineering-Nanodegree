# Project: Data Modeling with Postgres

## Project Description

The aim of this project is to put below things into practice 
- Relational Data Modeling
- Data Modeling using Postgres
- Building an ETL pipeline using Python


## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users are listening to. 
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project.
Your role is to create a database schema and ETL pipeline for this analysis.


## Database Schema

Star Schema was used for this project.Star schemas are organized into fact and dimension tables in whcih facts are measurable data about an event while dimensions are attrributes to describe the fact data


Motivation for choosing relational database
- Data structure is known beforehand and it is static
- Data volume is smaller and relational data base can be used for its simplicity
- Analytics team wants do do analysis and SQL suits best for this purpose
- Ability to perform joins and aggregations which are required during analysis




## Project folders and files

1. **data** Folder where the data which needs to be analysed resides.
2. **sql_queries.py** Consists of all the sql queries - DROP, CREATE, INSERT,SELECT
3. **create_tables.py** Drops and creates the database and tables.
4. **test.ipynb** Displays first few rows of tables 
5. **etl.ipynb** Reads and processes a single file from song_data and log_data and inserts data into respective tables. 
6. **etl.py** Reads and processes all the files from data folder and inserts data into respective tables


## Detailed description of ETL pipeline

1. First we start with creating the required the database and tables by running the **create_tables.py** script
2. In **etl.py** script we connect to the database which we created in the previous step
3. We then walkthrough the files within song_data folder and pass the file to function **process_song_file()**
4. Within **process_song_file()** we use pandas library to read the file and insert data into songs and artists table
5. We then follow the same steps to process the files within log_data folder by passing the files to fucntion **prcoess_log_file()**
6. Within **prcoess_log_file()** we do filtering based on "Next Song Action" and also do some datatype conversions.
7. FInally we then load the users and songplays table.


