# Project: Data Modeling with Postgres

## Description
This project implements a PostgreSQL database from a collection of activity logs provided as JSON files by a bogus startup called 'Sparkify', that owns a music streaming app and it is interested in performing queries and analytics on this user-generated data, in order to understand in particular, what songs users are listening to.
The project includes the script to create the database tables, the SQL queries for creation and insertion and the ETL pipeline to extract the data from the JSON logs.

## Structure
* `data` : contains the dataset in the form of JSON files, divided into `song_data` and `log_data`. The former contains metadata about a song and the artist of that song, while the latter simulates activity logs from the music streaming app. (To have more information about the records and their attributes, refer to `etl.ipynb`).
* `create_tables.py` : script that creates the database 'sparkifydb' and the tables
* `etl.ipynb` : Jupyter notebook that contains the ETL pipeline - with some useful visualization
* `etl.py` : script that implements the functions tested in the above notebook
* `sql_queries.py` : contains the SQL queries for table creation and values insertion, used in the phase of creation and pipeline
* `test.ipynb` : notebook that contains some test queries, used to verify correctness of ETL pipeline functions

## Database schema
The 'sparkifydb' database implements a star schema, with one fact table (_songplays_) and multiple dimension tables (_users_, _songs_, _artists_ and _times_).

***songplays***
* songplay_id SERIAL PRIMARY KEY 
* start_time TIMESTAMP REFERENCES times(start_time) 
* user_id INT REFERENCES users(user_id)
* level VARCHAR
* song_id VARCHAR REFERENCES songs(song_id)
* artist_id VARCHAR REFERENCES artists(artist_id)
* session_id INT
* location VARCHAR
* user_agent VARCHAR

***users***
* user_id INT PRIMARY KEY
* first_name VARCHAR 
* last_name VARCHAR
* gender VARCHAR
* level VARCHAR

***songs***
* song_id VARCHAR PRIMARY KEY 
* title VARCHAR
* artist_id VARCHAR
* year INT
* duration INT

***artists***
* artist_id VARCHAR PRIMARY KEY
* name VARCHAR
* location VARCHAR
* latitude FLOAT
* longitude FLOAT

***times***
* start_time TIMESTAMP PRIMARY KEY
* hour INT 
* day INT 
* week INT 
* month INT 
* year INT 
* weekday INT

## ETL Pipeline

1. Process all the JSON files contained in `data/song_data` as Pandas dataframes
2. Extract values corresponding to attributes ['song_id', 'title', 'artist_id', 'year', 'duration'] to fill _songs_ table
3. Extract values corresponding to attributes ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'] to fill _artists_ table
4. Process all the JSON files contained in `data/log_data` as Pandas dataframes
5. Filter log data according to 'NextSong' attribute
6. Convert _ts_ column from timestamp format to datetime
7. Extract datetime properties from the new column to fill _times_ table with values of attributes ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
8. Extract values corresponding to attributes ['userId', 'firstName', 'lastName', 'gender', 'level'] to fill _users_ tables
9. Use information from _songs_ and _artists_ tables to fill _songplays_ table with values corresponding to attributes ['ts', 'userId', 'level', 'songId', 'artistId', 'sessionId', 'location', 'userAgent'].