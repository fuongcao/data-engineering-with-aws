class TableCreateqlQueries:
    STAGING_EVENTS_TABLE_CREATE= ("""
        CREATE TABLE IF NOT EXISTS staging_events(
            artist VARCHAR(256),
            auth VARCHAR(256),
            firstname VARCHAR(256),
            gender VARCHAR(256),
            iteminsession INT,
            lastname VARCHAR(256),
            length DECIMAL(18,0),
            level VARCHAR(256),
            location VARCHAR(256),
            method VARCHAR(256),
            page VARCHAR(256),
            registration BIGINT,
            sessionid INT,
            song VARCHAR(256),
            status INT,
            ts BIGINT,
            useragent VARCHAR(256),
            userid VARCHAR(256)
        ) 
    """)

    STAGING_SONGS_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            artist_id VARCHAR(256),
            artist_latitude DECIMAL(18,0),
            artist_longitude DECIMAL(18,0),
            artist_location VARCHAR(256),
            artist_name VARCHAR(256),
            duration DECIMAL(18,0),
            num_songs INT,
            song_id VARCHAR(256),
            title VARCHAR(256),
            year INT
        )
    """)

    SONGPLAY_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id VARCHAR(32) NOT NULL PRIMARY KEY distkey,
            start_time DATETIME NOT NULL sortkey,
            user_id VARCHAR(256),
            level VARCHAR(256),
            song_id VARCHAR(256),
            artist_id VARCHAR(256),
            session_id INT,
            location VARCHAR(256),
            user_agent VARCHAR(256)
        )
    """)

    USER_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(256) NOT NULL,
            first_name VARCHAR(256) NOT NULL,
            last_name VARCHAR(256) NOT NULL,
            gender VARCHAR(256) NOT NULL,
            level VARCHAR(256) NOT NULL
        )
        diststyle all
    """)

    SONG_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR(256) NOT NULL,
            title VARCHAR(256) NOT NULL,
            artist_id VARCHAR(256) NOT NULL,
            year INT,
            duration DECIMAL(18,0)
        )
        diststyle all
    """)

    ARTIST_TABLE_CREATE = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR(256) NOT NULL,
            name VARCHAR(256) NOT NULL,
            location VARCHAR(256),
            lattitude DECIMAL(18,0),
            longitude DECIMAL(18,0)
        )
        diststyle all
    """)

    TIME_TABLE_CREATE = ("""
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

    # QUERY LISTS

    LIST_OF_QUERIES = { 
        "STAGING_EVENTS_TABLE_CREATE": STAGING_EVENTS_TABLE_CREATE,
        "STAGING_SONGS_TABLE_CREATE": STAGING_SONGS_TABLE_CREATE, 
        "SONGPLAY_TABLE_CREATE": SONGPLAY_TABLE_CREATE, 
        "USER_TABLE_CREATE": USER_TABLE_CREATE, 
        "SONG_TABLE_CREATE": SONG_TABLE_CREATE, 
        "ARTIST_TABLE_CREATE": ARTIST_TABLE_CREATE, 
        "TIME_TABLE_CREATE": TIME_TABLE_CREATE 
    }