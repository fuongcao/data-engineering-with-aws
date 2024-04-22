import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ROLE_ARN = config.get('IAM_ROLE', 'ARN')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

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
    CREATE TABLE IF NOT EXISTS staging_events(
        artist TEXT,
        auth TEXT,
        firstname TEXT,
        gender TEXT,
        iteminsession INT,
        lastname TEXT,
        length DECIMAL,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration BIGINT,
        sessionid BIGINT,
        song TEXT,
        status INT,
        ts BIGINT,
        useragent TEXT,
        userid TEXT
    ) 
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id TEXT,
        artist_latitude DECIMAL,
        artist_location TEXT,
        artist_longitude DECIMAL,
        artist_name TEXT,
        duration DECIMAL,
        num_songs INT,
        song_id TEXT,
        title TEXT,
        year INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT NOT NULL IDENTITY(0, 1) distkey,
        start_time DATETIME NOT NULL sortkey,
        user_id TEXT,
        level TEXT,
        song_id TEXT,
        artist_id TEXT,
        session_id BIGINT,
        location TEXT,
        user_agent TEXT
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT NOT NULL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender TEXT NOT NULL,
        level TEXT NOT NULL
    )
    diststyle all
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT NOT NULL,
        title TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        year INT,
        duration DECIMAL
    )
    diststyle all
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT NOT NULL,
        name TEXT NOT NULL,
        location TEXT,
        lattitude DECIMAL,
        longitude DECIMAL
    )
    diststyle all
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time DATETIME NOT NULL sortkey,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    )
    diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto ignorecase'
""").format(LOG_DATA, ROLE_ARN)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto ignorecase' 
""").format(SONG_DATA, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + ste.ts/1000 * INTERVAL '1 second' AS start_time,
        ste.userid,
        ste.level,
        sts.song_id,
        sts.artist_id,
        ste.sessionid,
        ste.location,
        ste.useragent
    FROM staging_events ste
        LEFT OUTER JOIN staging_songs sts
            ON ste.artist = sts.artist_name
            AND ste.song = sts.title
    WHERE ste.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT 
        userid,
        firstname,
        lastname,
        gender,
        level
    FROM staging_events
    WHERE firstname IS NOT NULL AND lastname IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        lattitude,
        longitude
    )
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    WITH timestamps AS (
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events)
    SELECT
        start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(dow from start_time)
    FROM timestamps
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
