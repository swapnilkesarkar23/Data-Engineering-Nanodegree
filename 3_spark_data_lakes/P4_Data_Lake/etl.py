import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date,TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3, processes, extracts songs and artist data
        and then again loads back to S3 in parquet format
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket data in parquet format will be stored
    """
    
    # get filepath to song data file
    song_data = "{}song_data/*/*/*/*.json".format(input_data)
    
    #define schmea for song data
    songSchema = R([
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_name",Str()),
    Fld("duration",Dbl()),
    Fld("num_songs",Int()),
    Fld("song_id", Str()),
    Fld("title", Str()),
    Fld("year", Int()),
    ])
    
    #read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    song_fields = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet("{}songs/".format(output_data))

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
                                                                 
    # write artists table to parquet files
    artists_table.write.parquet("{}artists/".format(output_data))
    
                                                               
def process_log_data(spark, input_data, output_data):
    """
        Process the log data dataset and creates the user table, time table and songsplay table
        param spark: SparkSession
        
        Parameters:
            input_data: path/to/files to process
            output_data: path/to/files to write the results Datasets
    """
    # get filepath to log data file
    log_data = "{}log_data/*/*/*.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")

    # extract columns for users table    
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()
                                                               
    # write users table to parquet files
    users_table.write.parquet("{}users/".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))
    time_table = df.select(col("start_time"), col("hour"), col("day"), col("week"), col("month"), col("year"), col("weekday")).dropDuplicates()                                                          
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet("{}time/".format(output_data))

    # read in song data and artists data to use for songplays table
    df_songs = spark.read.parquet("{}songs/*/*/*".format(output_data))
    df_artists = spark.read.parquet("{}artists/*".format(output_data))
    
    # extract columns from joined song and log datasets to create songplays table
    songs_logs = df.join(df_songs, (df.song == df_songs.title))
    songplays = songs_logs.join(df_artists, (songs_logs.artist == df_artists.name)).drop(df_artists.location)
    
    songplays_table = songplays.select(
    col('start_time').alias('start_time'),
    col('userId').alias('user_id'),
    col('level').alias('level'),
    col('song_id').alias('song_id'),
    col('artist_id').alias('artist_id'),
    col('sessionId').alias('session_id'),
    col('location').alias('location'),
    col('userAgent').alias('user_agent'),
    col('year').alias('year'),
    col('month').alias('month')).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet("{}songplays/".format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://udacity-sparkdatalake-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
