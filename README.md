# Project: Data Warehouse for a music streaming startup, Sparkify.
Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, I had tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

<img src="https://miro.medium.com/max/1100/0*Dnt6wUWlARdI1wim"/>

## Project Description
To complete the project, I was needed to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Project Datasets
I worked with 3 datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
This third file s3://udacity-dend/log_json_path.jsoncontains the meta information that is required by AWS to correctly load s3://udacity-dend/log_data

### Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.
### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset you'll be working with are partitioned by year and month.

## Schema for Song Play Analysis
Using the song and event datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
* songplays - records in event data associated with song plays i.e. records with page NextSong
* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
* users - users in the app:          
    user_id, first_name, last_name, gender, level
* songs - songs in music database:        
    song_id, title, artist_id, year, duration
* artists - artists in music database:     
    artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units        
    start_time, hour, day, week, month, year, weekday
    
    
## The project template:

* create_table.py is where I created the fact and dimension tables for the star schema in Redshift.
* etl.py is where I loadED data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
* sql_queries.py is where I defined the SQL statements, which will be imported into the two other files above.
* README.md is this file.

## Some words about the schema:
first we took the data from the source(amazon s3) and pot it almost as it is into a staging tables. it is similar to ELT process, where the transformation executed in the end.
then we moved all the data to our final tables which have build in a star schema. 
the star schema is very simple to query and ideal for OLAP processes.
(i think that we could improve the schema if we will add to the time table an "event_id" colunmn)