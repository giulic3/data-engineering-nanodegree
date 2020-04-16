import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    '''
    # For local deploy
    spark = SparkSession \
    .builder \
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR") # Set log level
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/"
    
    # enforce schema
    song_schema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType())
    ])
    # read song data file
    df = spark.read.schema(song_schema).json(song_data)
    
    # print schema and table
    #print("song_data schema")
    #df.printSchema()
    #print("song_data df")
    #df.show(5)
    
    df.createOrReplaceTempView("songs")
    #df.registerTempTable("songs")
    
    # extract columns to create songs table
    songs_df = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs
        """) # No ';' at the end of a query
    
    #print("songs_df: ")
    #songs_df.show(5)
    
    # write songs table to parquet files partitioned by year and artist
    songs_df.write.partitionBy("year", "artist_id").format("parquet").save(output_data + "songs_table.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_df = spark.sql("""
        SELECT 
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM songs
     """) 
    
    print("artists_df: ")
    artists_df.show(5)
    
    # write artists table to parquet files
    artists_df.write.format("parquet").save(output_data + "artists_table.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "/log_data/"

    # If I specify log_schema before, since userId can be null/empty, the read returns null values everywhere
    '''
    log_schema = StructType([
        StructField("artist", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", IntegerType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", TimestampType()),
        StructField("userAgent", StringType()),
        StructField("userId", IntegerType())
    ])
    ''' 
    # read log data file
    # df = spark.read.schema(log_schema).json(log_data)
    df = spark.read.json(log_data)
    
    # print schema and table
    # print("log_data schema")
    # df.printSchema()
    # df.show(5)
    
    df.createOrReplaceTempView("logs")
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_df = spark.sql("""
        SELECT 
            userId AS user_id, 
            firstName AS first_name, 
            lastName AS last_name,
            gender,
            level
         FROM logs
    """)
    # print("users_df: ")
    # users_df.show(5)
    
    # write users table to parquet files
    users_df.write.format("parquet").save(output_data + "users_table.parquet", mode="overwrite")
    
    # create timestamp column from original timestamp column
    df = df.withColumn(
        "datetime",
        to_date(from_unixtime((col("ts") / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast("Date")
    )
        
    print("New logs schema, after datetime update: ")
    df.printSchema()
    df.show(5)
    
    # extract columns to create time table
    times_df = df.withColumn("hour", hour(col("datetime"))) \
          .withColumn("day", dayofmonth(col("datetime"))) \
          .withColumn("week", weekofyear(col("datetime"))) \
          .withColumn("month", month(col("datetime"))) \
          .withColumn("year", year(col("datetime"))) \
          .withColumn("weekday", dayofweek(col("datetime"))) \
          .select(
            col("datetime").alias("start_time"),
            col("hour"),
            col("day"),
            col("week"),
            col("month"),
            col("year"),
            col("weekday")
          )
    
    # print("times_df: ")
    # times_df.show(5)
    
    # write time table to parquet files partitioned by year and month
    times_df.write.partitionBy("year", "month").format("parquet").save(output_data + "times_table.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_data = input_data + "/song_data/*/*/*/"
    song_df = spark.read.json(song_data)
    print("song_df schema: ")
    song_df.printSchema()
    song_df.show(5)
    
    # first join logs and songs
    df = df.withColumn("songplay_id", monotonically_increasing_id()) # Create id as 'serial'
    joined_df = df.join(song_df, (song_df.title == df.song) & (df.artist == song_df.artist_name))
    
    print("joined_df: ")
    joined_df.show(5)
    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = joined_df.select(
                "songplay_id",
                col("datetime").alias("start_time"),
                col("userId").alias("user_id"),
                "level",
                "song_id",
                "artist_id",
                col("sessionId").alias("session_id"),
                "location",
                col("userAgent").alias("user_agent")
            )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_df = songplays_df \
                .withColumn("year", year(col("start_time"))) \
                .withColumn("month", month(col("start_time")))
    
    print("songplays_df: ")
    songplays_df.show(5)
    
    songplays_df.write.partitionBy("year", "month").format("parquet").save(output_data + "songplays_table.parquet", mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    # Deploy Spark locally, must point to the dir containing JSON files
    # input_data = "data/"
    # output_data = "."
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
