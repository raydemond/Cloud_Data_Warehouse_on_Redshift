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
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId int,
        song varchar,
        status int,
        ts timestamp ,
        userAgent varchar,
        userId int  
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY, 
        start_time timestamp NOT NULL SORTKEY REFERENCES time(start_time), 
        user_id int NOT NULL DISTKEY REFERENCES users(user_id), 
        level varchar NOT NULL,
        song_id varchar REFERENCES songs(song_id), 
        artist_id varchar REFERENCES artists(artist_id), 
        session_id int NOT NULL, 
        location varchar, 
        user_agent varchar
         );
""")


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY DISTKEY, 
        first_name varchar, 
        last_name varchar, 
        gender varchar SORTKEY, 
        level varchar 
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY, 
        title varchar, 
        artist_id varchar NOT NULL, 
        year int SORTKEY, 
        duration float
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY, 
        name varchar NOT NULL, 
        location varchar, 
        latitude float, 
        longitude float
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp PRIMARY KEY SORTKEY DISTKEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL
        );
""")




# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    FORMAT AS JSON {}
    TIMEFORMAT 'epochmillisecs'
    REGION 'us-west-2'
""").format(config.get('S3','LOG_DATA'), 
            config.get('IAM_ROLE','ARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {}
    FORMAT AS JSON 'auto'
    REGION 'us-west-2'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, 
                           level, song_id, artist_id, 
                           session_id, location, user_agent) 
    SELECT DISTINCT ts AS start_time,
           userId AS user_id,
           level,
           song_id,
           artist_id,
           sessionId AS session_id,
           location,
           userAgent AS user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s
    ON e.length = s.duration AND e.song = s.title and e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT userId AS user_id,
           firstName AS first_name,
           lastName AS last_name,
           gender,
           level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id,
           artist_name AS name,
           artist_location AS location,
           artist_latitude AS latitude,
           artist_longitude AS longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    SELECT start_time AS start_time,
           EXTRACT(hour FROM start_time) AS hour,
           EXTRACT(day FROM start_time) AS day,
           EXTRACT(week FROM start_time) AS week,
           EXTRACT(month FROM start_time) AS month,
           EXTRACT(year FROM start_time) AS year,
           EXTRACT(weekday FROM start_time) AS weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, 
                        song_table_create, artist_table_create, time_table_create, songplay_table_create,]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, 
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
