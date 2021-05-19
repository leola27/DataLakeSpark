# Udacity Data Engineer Nanodegree project

## Data Lake Spark ETL

### Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Database schema design and ETL process

#### Fact Table
songplays (records in log data associated with song plays): songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### Dimension Tables
users (users in the app): user_id, first_name, last_name, gender, level
songs (songs in music database): song_id, title, artist_id, year, duration
artists (artists in music database): artist_id, name, location, lattitude, longitude
time (timestamps of records in songplays broken down into specific units): start_time, hour, day, week, month, year, weekday

### Files in repository

#### dl.cfg file
Configuration File containing AWS credentials

#### etl.py file
Contains functions to download json data from S3, transform it and write into parquet files back into s3

### How to run python script
The script is ran from an EMR cluster and requires the creation of IAM role, EMR cluster and S3 bucket for input/output. Once the EMR cluster is created, the script has to be copied to it via SSH channel and run using a command spark-submit

#### IAM Role
Can be created from AWS dashboard in  AWS management console, or using aws configure commands from CLI (https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

#### EMR Cluster
Was created via CLI command, for example: 

*aws emr create-cluster --name <cluster_name> \
 --use-default-roles --release-label emr-5.28.0  \
--instance-count 3 --applications Name=Spark Name=Zeppelin  \
--bootstrap-actions Path="s3://bootstrap.sh" \
--ec2-attributes KeyName=<Key-pair-file-name>, SubnetId=<subnet-Id> \
--instance-type m5.xlarge --log-uri s3:///emrlogs/*

#### S3 bucket
Was created via  AWS management console