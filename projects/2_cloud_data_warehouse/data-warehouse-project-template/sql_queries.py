import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
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
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
