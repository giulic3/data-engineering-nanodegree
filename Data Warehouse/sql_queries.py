import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES
# TODO Try query optimization using sortkey/distkey
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INT,
        song VARCHAR,
        status INTEGER,
        ts TIMESTAMP,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
        start_time TIMESTAMP REFERENCES times(start_time), 
        user_id INT REFERENCES users(user_id), 
        level VARCHAR, 
        song_id VARCHAR REFERENCES songs(song_id), 
        artist_id VARCHAR REFERENCES artists(artist_id),
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year INT, 
        duration INT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        latitude FLOAT, 
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS times (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT
    );
""")

# STAGING TABLES
# Log data
staging_events_copy = ("""
    copy staging_events from {log_data}
    credentials 'aws_iam_role={role_arn}'
    region '{aws_region}' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(log_data=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'],
           aws_region=config['CLUSTER']['AWS_REGION'], log_json_path=config['S3']['LOG_JSON_PATH'])

# Song data
staging_songs_copy = ("""
    copy staging_songs from {song_data}
    credentials 'aws_iam_role={role_arn}'
    region '{aws_region}' format as JSON 'auto';
""").format(song_data=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'], aws_region=config['CLUSTER']['AWS_REGION'])

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        DISTINCT(se.ts) AS start_time,
        se.userId AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT 
        DISTINCT(userId) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events 
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT 
        artist_id,
        artist_name AS name
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(dayofweek FROM start_time) AS weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
