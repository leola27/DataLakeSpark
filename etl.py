import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.functions import monotonically_increasing_id
import os
from datetime import datetime as dt

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, song_data, output_data):

    # read song data file
    song_data_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_data_df.select("song_id", "title", "artist_id", "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs.parquet"))

    # extract columns to create artists table
    artists_table = song_data_df.select("artist_id", "artist_name", "artist_location", "artist_latitude",
                                        "artist_longitude")

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"))


def process_log_data(spark, song_data, log_data, output_data):

    # read log data file
    log_data_df = spark.read.json(log_data)

    # read song data file
    song_data_df = spark.read.json(song_data)

    # filter by actions for song plays
    log_data_next_song = log_data_df.where(log_data_df.page == "NextSong")

    # extract columns for users table    
    users_table = log_data_next_song.select("userid", "firstName", "lastName", "gender", "level")

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: dt.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    log_data_timestamp = log_data_next_song.withColumn("timestamp", get_timestamp("ts"))

    # create datetime column from original timestamp column
    log_data_datetime = log_data_timestamp.withColumn("datetime", F.to_timestamp("timestamp"))

    # add  hour, day, week, month, year, weekday
    log_data_datetime = log_data_datetime.withColumn("hour", F.hour("datetime")) \
        .withColumn("month", F.month("datetime")) \
        .withColumn("year", F.year("datetime")) \
        .withColumn("day", F.dayofmonth("datetime")) \
        .withColumn("week", F.weekofyear("datetime")) \
        .withColumn("weekday", F.dayofweek("datetime"))
    log_data_datetime.toPandas()

    # extract columns to create time table
    time_table = log_data_datetime.select("datetime", "hour", "day", "week", "month", "year", "weekday").select(
        "datetime", "hour", "day", "week", "month", "year", "weekday")

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time.parquet"))

    # read in song data to use for songplays table
    joint_data = log_data_datetime.join(song_data_df, (log_data_datetime.song == song_data_df.title) & (
            log_data_datetime.artist == song_data_df.artist_name), "inner")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joint_data.select("datetime", "userId", "level", "song_id", "artist_id", "sessionId",
                                        "artist_location", "userAgent")
    songplays_table = songplays_table.withColumn("month", F.month("datetime")) \
        .withColumn("year", F.year("datetime")) \
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays.parquet"))

    spark.stop()


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://outputdatadatalake/"

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*.json")

    process_song_data(spark, song_data, output_data)
    process_log_data(spark, song_data, log_data, output_data)


if __name__ == "__main__":
    main()
