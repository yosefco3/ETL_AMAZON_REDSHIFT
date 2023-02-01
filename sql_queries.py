import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
BUCKET = config.get('S3', 'BUCKET')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES


# staging: staging_events
staging_events_table_create = ("""CREATE TABLE staging_events
  (
     event_id        INT IDENTITY(0, 1) PRIMARY KEY,
     artist          VARCHAR(255),
     auth            VARCHAR(50),
     first_name      VARCHAR(255),
     gender          VARCHAR(1),
     item_in_session INTEGER,
     last_name       VARCHAR(255),
     length          DOUBLE PRECISION,
     level           VARCHAR(50),
     location        VARCHAR(255),
     method          VARCHAR(25),
     page            VARCHAR(35),
     registration    VARCHAR(50),
     session_id      BIGINT,
     song            VARCHAR(255),
     status          INTEGER,
     ts              VARCHAR(50),
     user_agent      TEXT,
     user_id         VARCHAR(100)
  );
""")

# staging: staging_songs
staging_songs_table_create = ("""CREATE TABLE staging_songs
             (
                          num_songs        INTEGER,
                          artist_id        VARCHAR(50),
                          artist_latitude  NUMERIC,
                          artist_longitude NUMERIC,
                          artist_location  VARCHAR(255),
                          artist_name      VARCHAR(255),
                          song_id          VARCHAR(50) PRIMARY KEY,
                          title            VARCHAR(255),
                          duration         NUMERIC,
                          year             INTEGER
);
""")



# Fact Table: songplays
songplay_table_create = ("""CREATE TABLE songplays
             (
                          songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                          start_time TIMESTAMP,
                          user_id    INTEGER NOT NULL,
                          level      VARCHAR(10),
                          song_id    VARCHAR(30) NOT NULL,
                          artist_id  VARCHAR(30) NOT NULL,
                          session_id INTEGER NOT NULL,
                          location   VARCHAR(100),
                          user_agent VARCHAR(200),
                          event_id   INTEGER
);
""")

# Dimension Table: users
user_table_create = ("""CREATE TABLE users
             (
                          user_id    VARCHAR(100) PRIMARY KEY,
                          first_name VARCHAR(20),
                          last_name  VARCHAR(20),
                          gender     CHAR(1),
                          level      VARCHAR(10)
);""")


# Dimension Table: songs
song_table_create = ("""CREATE TABLE songs
             (
                          song_id   VARCHAR(50) PRIMARY KEY,
                          title     VARCHAR(300),
                          artist_id VARCHAR(30),
                          year      INTEGER,
                          duration  NUMERIC(10,5)
);""")

# Dimension Table: artists
artist_table_create = ("""CREATE TABLE artists
             (
                          artist_id VARCHAR(50) PRIMARY KEY,
                          NAME      VARCHAR(300),
                          location  VARCHAR(200),
                          latitude  NUMERIC(10,5),
                          longitude NUMERIC(10,5)
);""")

# Dimension Table: time
time_table_create = ("""CREATE TABLE time
             (
                          event_id INT PRIMARY KEY,
                          start_time TIMESTAMP,
                          hour    INTEGER,
                          day     INTEGER,
                          week    INTEGER,
                          month   INTEGER,
                          year    INTEGER,
                          weekday INTEGER
);""") 



# STAGING TABLES

staging_events_copy  = ("""copy staging_events from  's3://{}/{}'
credentials 'aws_iam_role={}'
 region 'us-west-2' 
 COMPUPDATE OFF STATUPDATE OFF
 JSON 's3://{}/{}'""").format(BUCKET,LOG_DATA, ARN, BUCKET,LOG_JSONPATH)



staging_songs_copy = ("""copy staging_songs from  's3://{}/{}'
credentials 'aws_iam_role={}'
    region 'us-west-2' 
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'
    """).format(BUCKET,SONG_DATA, ARN)

  
# FINAL TABLES


songplay_table_insert = ("""INSERT INTO songplays
            (start_time,
             user_id,
             LEVEL,
             song_id,
             artist_id,
             session_id,
             location,
             user_agent)
SELECT DISTINCT CASE
                  WHEN e.ts :: NUMERIC < 99999999999 THEN
                  To_timestamp(( e.ts / 1000 ) :: text, 'YYYY-MM-DD HH24:MI:SS')
                  ELSE NULL
                END AS start_time,
                Cast(e.user_id AS INTEGER),
                e.LEVEL,
                s.song_id,
                s.artist_id,
                e.session_id,
                e.location,
                e.user_agent
FROM   staging_events e
       join staging_songs s
         ON e.song = s.title
            AND e.artist = s.artist_name
WHERE  e.song IS NOT NULL; 
""")


user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
                first_name,
                last_name,
                gender,
                level
FROM   staging_events e
       JOIN staging_songs s
         ON e.song = s.title
            AND e.artist = s.artist_name
WHERE  e.song IS NOT NULL; 
""")


    

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM   staging_songs 
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""INSERT INTO TIME
            (start_time,
             hour,
             day,
             week,
             month,
             year,
             weekday,
             event_id)
SELECT start_time,
       Extract(hr FROM start_time)      AS hour,
       Extract(d FROM start_time)       AS day,
       Extract(w FROM start_time)       AS week,
       Extract(mon FROM start_time)     AS month,
       Extract(yr FROM start_time)      AS year,
       Extract(weekday FROM start_time) AS weekday,
       event_id
FROM   (SELECT CASE
                 WHEN ts :: NUMERIC < 99999999999 THEN
                 To_timestamp(( ts / 1000 ) :: text, 'YYYY-MM-DD HH24:MI:SS')
                 ELSE NULL
               END AS start_time,
               event_id
        FROM   staging_events
        WHERE  start_time IS NOT NULL) AS subquery;""")


# QUERY LISTS

# this two lines are for the case we want to drop and create all the tables except the staging ones:
drop_without_staging = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_without_staging = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]


drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]


create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
