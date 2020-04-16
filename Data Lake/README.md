# Project: Data Lake

## Description
This project implements a data lake using AWS (S3, EMR) and Spark, for a music streaming startup called _Sparkify_, that has grown their user base to a point that a date warehouse is no more the most suitable solution for their data.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
The project implements an ETL pipeline that extracts this data from S3, processes them using Spark into analytics table and loads the data back into S3 as a dimensional model.

## Structure
* `data` : contains the dataset, `song_data` and `log_data`
* `dl.cfg` : contains user AWS credentials (ACCESS_KEY, SECRET_ACCESS_KEY), specified without any single or double quotes
* `etl.py` : implements the ETL pipeline, reading data from storage (S3), processing it using Spark and writing it back to storage

## Database schema

```
songplays: 
 |-- songplay_id: long (nullable = false)
 |-- start_time: date (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```
```
users:
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
```
```
songs:
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- duration: double (nullable = true)
```
```
artists:                                             
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
```
```
times
 |-- start_time: date (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- weekday: integer (nullable = true)
```

## ETL Pipeline
The ETL pipeline implemented in the `etl.py` file consists of three functions:
* `create_spark_session()` : to instantiate Spark
* `process_song_data()` : to read song-related data and create 2 of the 5 tables (_songs_ and _artists_)
* `process_log_data()` : to read log-related data and create the remaining 3 tables (_times_, _users_ and _songplays_).


## Access to Spark
The project was tested first locally, using the Jupyter notebook workspace provided by Udacity on a part of the dataset, and later was tested using AWS (S3 and EMR services) on the full dataset.
The EMR cluster was executed without EC2 keys, with a _spark_ user created specifically for the project, whose credentials must be specified in the configuration file `dl.cfg`.

NB: The user must have suitable policy permissions to use S3 and EMR.

In this project both Spark SQL and Spark DataFrames were used.