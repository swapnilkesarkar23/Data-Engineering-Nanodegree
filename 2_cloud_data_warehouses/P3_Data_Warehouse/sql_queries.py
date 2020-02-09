import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events
    (artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession varchar,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    sessionId integer,
    song varchar,
    status integer,
    ts timestamp,
    userAgent varchar,
    userId integer)
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs
    (num_songs integer,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer)
""")

songplay_table_create = ("""
    CREATE TABLE songplays
    (songplay_id integer IDENTITY(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL SORTKEY DISTKEY,
    user_id integer NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id integer,
    location varchar,
    user_agent varchar)
""")

user_table_create = ("""
    CREATE TABLE users
    (user_id integer NOT NULL SORTKEY PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL,
    gender varchar,
    level varchar)
""")

song_table_create = ("""
    CREATE TABLE songs
    (song_id varchar NOT NULL SORTKEY PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL,
    year integer, 
    duration float NOT NULL)
""")

artist_table_create = ("""
    CREATE TABLE artists
    (artist_id varchar NOT NULL SORTKEY PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar,
    latitude float, 
    longitude float)
""")

time_table_create = ("""
    CREATE TABLE time
    (start_time date NOT NULL SORTKEY PRIMARY KEY, 
    hour integer, 
    day integer, 
    week integer, 
    month integer, 
    year integer,
    weekday integer)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {bucket}
    region 'us-west-2'
    iam_role {role_arn}
    json {json_path}
    timeformat as 'epochmillisecs'
""").format(bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs
    from {bucket}
    region 'us-west-2'
    iam_role {role_arn}
    json 'auto'
""").format(bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT(a.ts) AS start_time,
    a.userId AS user_id,
    a.level AS level,
    b.song_id AS song_id,
    b.artist_id AS artist_id,
    a.sessionId AS session_id,
    a.location AS location,
    a.userAgent AS user_agent
    FROM staging_events a, staging_songs b
    WHERE a.song = b.title
    AND a.artist = b.artist_name
    AND a.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT (userId) AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page = 'NexSong'
""")

song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
    FROM staging_songs WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(start_time) AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dayofweek FROM start_time) AS weekday
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
