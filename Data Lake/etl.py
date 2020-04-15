import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType


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
    
    # spark.sparkContext.setLogLevel("ERROR") # Set log level
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = input_data + "/song_data/"
    song_data = input_data = "data/song_data/A/A/A/" # Deploy Spark locally, must point to the dir containing JSON files
    
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
    print("song_data schema")
    df.printSchema()
    df.show(5)
    
    df.createOrReplaceTempView("songs")
    #df.registerTempTable("songs")
    
    # extract columns to create songs table
    songs_df = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs;
        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_df.write.partitionBy("year", "artist_id").format("parquet").save("songs_table.parquet")

    # extract columns to create artists table
    artists_df = spark.sql("""
        SELECT 
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM songs;
     """) 
    
    # write artists table to parquet files
    artists_df.write.format("parquet").save("artists_table.parquet")

'''
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table
'''

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
