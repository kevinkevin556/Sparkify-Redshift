import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop   = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop    = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop         = "DROP TABLE IF EXISTS songplay"
user_table_drop             = "DROP TABLE IF EXISTS users"
song_table_drop             = "DROP TABLE IF EXISTS songs"
artist_table_drop           = "DROP TABLE IF EXISTS artists"
time_table_drop             = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        PRIMARY KEY (songplay_id),
        songplay_id     INT             IDENTITY(0,1),   
        artist          VARCHAR, 
        auth            VARCHAR,
        user_first_name VARCHAR,
        gender          VARCHAR,
        item_in_session INT,
        user_last_name  VARCHAR,
        length          NUMERIC,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    NUMERIC,
        session_id      INT,
        song            VARCHAR,   
        status          INT,
        ts              NUMERIC,
        user_agent      VARCHAR,
        user_id         INT)
    DISTKEY(songplay_id)
    SORTKEY(songplay_id);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        PRIMARY KEY (song_id),
        artist_id           VARCHAR,
        artist_latitude     NUMERIC,
        artist_location     VARCHAR,
        artist_longitude    NUMERIC,
        artist_name         VARCHAR,
        duration            NUMERIC,
        num_songs           INT,
        song_id             VARCHAR,
        title               VARCHAR,
        year                INT)
    DISTKEY(num_songs)
    SORTKEY(num_songs);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        PRIMARY KEY (songplay_id),
        songplay_id     INT         NOT NULL,
        start_time      TIMESTAMP   NOT NULL, 
        user_id         INT         NOT NULL,
        level           VARCHAR,    
        song_id         VARCHAR     NOT NULL,
        artist_id       VARCHAR     NOT NULL,
        session_id      INT         NOT NULL,
        location        VARCHAR,
        user_agent      VARCHAR)
    DISTKEY(songplay_id)
    SORTKEY(songplay_id);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        PRIMARY KEY (user_id),
        user_id          INT         NOT NULL,
        user_first_name  VARCHAR     NOT NULL,
        user_last_name   VARCHAR     NOT NULL,
        gender           VARCHAR,
        level            VARCHAR     NOT NULL)
    DISTKEY(user_id)
    SORTKEY(user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        PRIMARY KEY (song_id),
        song_id     VARCHAR     NOT NULL, 
        title       VARCHAR     NOT NULL, 
        artist_id   VARCHAR     NOT NULL,
        year        INT, 
        duration    NUMERIC)
    DISTKEY(song_id)
    SORTKEY(song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        PRIMARY KEY (artist_id),
        artist_id   VARCHAR     NOT NULL,
        name        VARCHAR     NOT NULL,
        location    VARCHAR,
        latitude   VARCHAR,
        longitude   VARCHAR)
    DISTKEY(artist_id)
    SORTKEY(artist_id)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        PRIMARY KEY (start_time),
        start_time  TIMESTAMP   NOT NULL,
        hour        INT         NOT NULL,
        day         INT         NOT NULL,
        week        INT         NOT NULL,
        month       INT         NOT NULL,
        year        INT         NOT NULL,
        weekday     INT         NOT NULL)
    DISTKEY(start_time)
    SORTKEY(start_time)
""")

# STAGING TABLES

staging_events_copy = ("""
           COPY staging_events 
           FROM {}
    CREDENTIALS 'aws_iam_role={}'
           JSON {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
           COPY staging_songs 
           FROM {}
    CREDENTIALS 'aws_iam_role={}'
           JSON 'auto';
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, 
                           song_id, artist_id,
                           user_id, session_id, level, location, user_agent)
         SELECT songplay_id, start_time, 
                song_id, artist_id,
                user_id, session_id, level, location, user_agent
           FROM (SELECT songplay_id,
                        TIMESTAMP 'epoch' +  ts/1000.0 * INTERVAL '1 seconds' AS start_time, 
                        song,
                        user_id, session_id, level, location, user_agent
                   FROM staging_events
                   WHERE page = 'NextSong') AS u
           JOIN (SELECT song_id, artist_id,
                        title AS song   
                   FROM staging_songs) AS s
             ON u.song = s.song 
""")

user_table_insert = ("""
    INSERT INTO users (user_id, 
                       user_first_name, user_last_name,
                       gender, level)
         SELECT c.user_id, 
                c.user_first_name, c.user_last_name,
                c.gender, c.level 
           FROM (SELECT DISTINCT user_id 
                            FROM staging_events) AS id
           JOIN (SELECT user_id, 
                        user_first_name, user_last_name,
                        gender, level 
                   FROM staging_events) AS c
             ON id.user_id = c.user_id
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,
                       title, year, duration,
                       artist_id)
         SELECT c.song_id,
                c.title, c.year, c.duration,
                c.artist_id
           FROM (SELECT DISTINCT song_id
                            FROM staging_songs) AS id
           JOIN (SELECT song_id,
                        title, year, duration,
                        artist_id
                   FROM staging_songs) AS c
             ON id.song_id = c.song_id
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                         name, location, latitude, longitude)
         SELECT c.artist_id,
                c.artist_name, c.artist_location, c.artist_latitude, c.artist_longitude
           FROM (SELECT DISTINCT artist_id
                            FROM staging_songs) AS id
           JOIN (SELECT artist_id,
                        artist_name, artist_location, artist_latitude, artist_longitude
                   FROM staging_songs) AS c
             ON id.artist_id = c.artist_id
""")


time_table_insert = ("""
    INSERT INTO time
         SELECT c.start_time,
                c.hour, c.day, c.week, c.month, c.year, c.weekday
           FROM (SELECT DISTINCT start_time
                            FROM songplays) AS t
           JOIN (SELECT start_time,
                        EXTRACT(HOUR FROM start_time) AS hour,
                        EXTRACT(DAY FROM start_time) AS day,
                        EXTRACT(HOUR FROM start_time) AS week,
                        EXTRACT(MONTH FROM start_time) AS month,
                        EXTRACT(YEAR FROM start_time) AS year,
                        EXTRACT(WEEKDAY FROM start_time) AS weekday
                   FROM songplays) AS c
             ON t.start_time = c.start_time
""")


# QUERY LISTS

create_table_queries = {
    "staging_events_table_create": staging_events_table_create,
    "staging_songs_table_create": staging_songs_table_create,
    "songplay_table_create": songplay_table_create,
    "user_table_create": user_table_create,
    "song_table_create": song_table_create,
    "artist_table_create": artist_table_create,
    "time_table_create": time_table_create
}

drop_table_queries = {
    "staging_events_table_drop": staging_events_table_drop,
    "staging_songs_table_drop": staging_songs_table_drop,
    "songplay_table_drop": songplay_table_drop,
    "user_table_drop": user_table_drop,
    "song_table_drop": song_table_drop,
    "artist_table_drop": artist_table_drop,
    "time_table_drop": time_table_drop
}

copy_table_queries = {
    "staging_events_copy": staging_events_copy,
    "staging_songs_copy": staging_songs_copy
}

insert_table_queries = {
    "songplay_table_insert": songplay_table_insert,
    "user_table_insert": user_table_insert,
    "song_table_insert": song_table_insert,
    "artist_table_insert": artist_table_insert,
    "time_table_insert": time_table_insert
}




# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
