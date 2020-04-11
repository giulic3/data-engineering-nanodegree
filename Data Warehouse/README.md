# Project: Data Warehouse

## Description
This project implements a data warehouse using some of the tools provided by AWS for a music streaming startup, Sparkify that has grown their user base and song database and want to move their processes and data onto the cloud.
The project implements an ETL pipeline that extracts their data, that currently resides as JSON logs in S3, load it to staging tables in Redshift and then transform this data into a set of dimensional tables on which analytics can be performed.

## Structure
* `create_tables.py` : script that connects to the Redshift database and create staging and analytics tables
* `dwh.cfg` : template file to fill with data relative to Redshift cluster and AWS user
* `etl.py` : script that implements the ETL pipeline, _extracts_ data from S3 bucket, _loads_ it to staging tables through COPY command and then _transforms_ it filling the analytics tables
* `run_analytics.py` : script that runs some simple queries to verify that the values were correctly inserted into the analytics tables
* `sql_queries.py` : contains the SQL queries for creation, insertion and analytics, to be imported and used.

## Database schema
For staging, two intermediate tables are used (_staging_events_ for `log_data` and _staging_songs_ for `song_data`), while the data warehouse implements a star schema, with one fact table (_songplays_) and multiple dimension tables (_users_, _songs_, _artists_ and _times_).

***staging_events***
* artist 			VARCHAR,
* auth				VARCHAR,
* firstName 		VARCHAR,
* gender 			VARCHAR,
* itemInSession 	INT,
* lastName 			VARCHAR,
* length 			FLOAT,
* level 			VARCHAR,
* location 			VARCHAR,
* method 			VARCHAR,
* page 				VARCHAR,
* registration 		FLOAT,
* sessionId 		INT,
* song 				VARCHAR,
* status 			INT,
* ts 				TIMESTAMP,
* userAgent 		VARCHAR,
* userId 			INT

***staging_songs***
* num_songs 		INT,
* artist_id 		VARCHAR,
* artist_latitude 	FLOAT,
* artist_longitude 	FLOAT,
* artist_location 	VARCHAR,
* artist_name 		VARCHAR,
* song_id 			VARCHAR,
* title 			VARCHAR,
* duration 			FLOAT,
* year 				INT

***songplays***
* songplay_id 		INT IDENTITY(0,1) PRIMARY KEY SORTKEY, 
* start_time 		TIMESTAMP REFERENCES times(start_time), 
* user_id 			INT REFERENCES users(user_id), 
* level 			VARCHAR, 
* song_id 			VARCHAR REFERENCES songs(song_id), 
* artist_id 		VARCHAR REFERENCES artists(artist_id),
* session_id 		INT, 
* location 			VARCHAR, 
* user_agent 		VARCHAR

***users***
* user_id 			INT PRIMARY KEY, 
* first_name 		VARCHAR NOT NULL, 
* last_name 		VARCHAR NOT NULL, 
* gender 			VARCHAR, 
* level 			VARCHAR NOT NULL

***songs***
* song_id 			VARCHAR PRIMARY KEY, 
* title 			VARCHAR NOT NULL SORTKEY, 
* artist_id 		VARCHAR NOT NULL DISTKEY, 
* year 				INT, 
* duration 			INT NOT NULL

***artists***
* artist_id 		VARCHAR PRIMARY KEY, 
* name 				VARCHAR NOT NULL SORTKEY, 
* location 			VARCHAR, 
* latitude 			FLOAT, 
* longitude 		FLOAT

***times***
* start_time 		TIMESTAMP PRIMARY KEY SORTKEY, 
* hour 				INT NOT NULL, 
* day 				INT NOT NULL, 
* week 				INT NOT NULL, 
* month 			INT NOT NULL, 
* year 				INT NOT NULL, 
* weekday 			INT NOT NULL

### About Redshift cluster creation
The cluster can be created using the GUI provided by the Amazon Redshift Console and the Quick Wizard, by attaching an appropriate IAM role and specifying a security group, or programmatically, using an IaC approach, e.g. through the Python SDK, boto3.
NB: The AWS region used for the cluster must be the same as the one of the S3 bucket, ('us-west-2' for this simple project) and the cluster must be made publicly available to allow connections to the endpoint from the outside.
