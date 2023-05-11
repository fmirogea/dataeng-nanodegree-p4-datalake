import configparser
from datetime import datetime
import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType, StringType
from pyspark.sql.functions import udf, col, from_unixtime, year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')


os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a new SparkSession with the required configuration.

    Returns:
    -------
    spark : SparkSession
        A SparkSession object with the required configuration.

    Raises:
    ------
    None

    Examples:
    --------
    >>> spark = create_spark_session()
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extract song data from input_data, transform it into a songs table and an artists table using Spark, 
    and write the tables to output_data in Parquet format. 

    Args:
    spark (SparkSession): A SparkSession object
    input_data (str): The file path for the input data
    output_data (str): The file path to write the output data

    Returns:
    None
    """

    # get filepath to song data file
    song_data = "s3://udacity-dend/song_data"
    
    # read song data file
    df = spark.read.json(song_data + "/*/*/*/*.json")

    # extract columns to create songs table
    songs_col = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df[songs_col]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+"output-songs-table")

    # extract columns to create artists table
    artists_col = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = df[artists_col]
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"output-artists-table")


def process_log_data(spark, input_data, output_data):
    """
    Processes log data and writes results to parquet files.

    Args:
        spark: SparkSession object.
        input_data (str): Path to input data.
        output_data (str): Path to output data.

    Returns:
        None
    """

    # get filepath to log data file
    log_data = "s3://udacity-dend/log_data/"

    # read log data file
    df = spark.read.json(log_data + "/*/*/*.json") 
    
    # filter by actions for song plays
    df = df[df["page"] == "NextSong"]

    # extract columns for users table    
    users_col = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = df[users_col]
    users_table_deduplicated = users_table.dropDuplicates()
    
    # write users table to parquet files
    users_table_deduplicated.write.parquet(output_data+"output-users-table")

    # create timestamp column from original timestamp column
    # Define a UDF to convert a Unix timestamp to a timestamp
    def from_unixtime_udf(ts):
        return datetime.datetime.fromtimestamp(ts/1000)
    
    get_timestamp = udf(from_unixtime_udf, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime", from_unixtime(col("ts")/1000))
    
    # extract columns to create time table
    time_col = ['timestamp', 'datetime']
    time_table = df[time_col]

    time_table = time_table.withColumn("hour", hour(col("timestamp")))
    time_table = time_table.withColumn("day", dayofmonth(col("timestamp")))
    time_table = time_table.withColumn("week", weekofyear(col("timestamp")))
    time_table = time_table.withColumn("month", month(col("timestamp")))
    time_table = time_table.withColumn("year", year(col("timestamp")))
    time_table = time_table.withColumn("weekday", dayofweek(col("timestamp")))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data+"output-time-table")

    # read in song data to use for songplays table
    song_path = output_data+"output-songs-table"
    song_df = spark.read.parquet(song_path)

    # extract columns from joined song and log datasets to create songplays table 
    ## Define a User Defined Function (UDF) to generate UUIDs
    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

    ## Add a new column to the DataFrame with unique UUIDs
    df_with_uuid = df.withColumn("songplay_id", uuid_udf())

    ## Join with song_df to get the song_id and artist_id columns
    songplays_table = df_with_uuid.join(song_df, df.song == song_df.title, "inner") \
                        .selectExpr("songplay_id",
                                    "timestamp as start_time",
                                    "userId as user_id",
                                    "level",
                                    "song_id",
                                    "artist_id",
                                    "sessionId as session_id",
                                    "location",
                                    "userAgent as user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_with_year_month = songplays_table.withColumn("year", year("start_time")).withColumn("month", month("start_time"))
    songplays_with_year_month.write.partitionBy("year", "month").parquet(output_data+"output-songplays-table")


def main():
    """
    This function orchestrates the ETL process for the Sparkify music streaming app data. 
    It creates a Spark session and sets the input and output data locations in an S3 bucket.
    Then, it processes the song data and log data using separate functions, and saves the results
    in the specified output location.

    Parameters:
    None

    Returns:
    None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-p4-data-lake-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
