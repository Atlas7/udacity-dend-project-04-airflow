class SqlQueries:
    
    
    table_drop = ("""
        DROP TABLE IF EXISTS {};
    """)

    staging_events_table_create= (
        """
        CREATE TABLE IF NOT EXISTS {} (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender VARCHAR,
            itemInSession INT,
            lastName VARCHAR,
            length DECIMAL,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration BIGINT,
            sessionId INT,
            song VARCHAR,
            status INT,
            ts TIMESTAMP,
            userAgent VARCHAR,
            userId BIGINT
        );
        """
    )

    staging_songs_table_create = (
        """
        CREATE TABLE IF NOT EXISTS {} (
            num_songs INT,
            artist_id VARCHAR,
            artist_latitude DOUBLE PRECISION,
            artist_longitude DOUBLE PRECISION,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration DECIMAL,
            year INT
        );
        """
    )
    
    
#     artists_table_columns = ("""(artist_id, name, location, lattitude, longitude)""") 
    artists_table_create = (
        """
        CREATE TABLE IF NOT EXISTS {} (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            location VARCHAR,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
        """
    )
    
#     songplays_table_columns = ("""
#         (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
#     """)
    
    songplays_table_create = (
        """
        CREATE TABLE IF NOT EXISTS {} (
            songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
            start_time TIMESTAMP,
            user_id BIGINT,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INT,
            location VARCHAR,
            user_agent VARCHAR
        );
        """
    )
    
    
#     songs_table_columns = ("""(song_id, title, artist_id, year, duration)""")
    
    songs_table_create = (
        """
        CREATE TABLE IF NOT EXISTS {} (
            song_id VARCHAR PRIMARY KEY,
            title VARCHAR NOT NULL,
            artist_id VARCHAR,
            year INT,
            duration DECIMAL NOT NULL
        );
        """
    )
    
#     time_table_columns = ("""(start_time, hour, day, week, month, year, weekday)""")
    
    time_table_create = (
        """
        CREATE TABLE IF NOT EXISTS {} (
            start_time TIMESTAMP PRIMARY KEY,
            hour INT,
            day INT,
            week INT,
            month INT,
            year INT,
            weekday INT
        );
        """
    )

    
#     users_table_columns = ("""(user_id, first_name, last_name, gender, level)""")
    
    users_table_create = (
        """
        CREATE TABLE IF NOT EXISTS {} (
            user_id BIGINT PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            level VARCHAR
        );
        """
    )
 
 
    # Reference: https://knowledge.udacity.com/questions/784957
    staging_events_copy = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
        TIMEFORMAT AS 'epochmillisecs'
        TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL
        COMPUPDATE OFF
        ;
    """
    )

    # Reference: https://knowledge.udacity.com/questions/784957
    staging_songs_copy = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
        COMPUPDATE OFF
        TRUNCATECOLUMNS EMPTYASNULL BLANKSASNULL
        ;
    """)


    songplays_table_insert = ("""
        INSERT INTO songplays (
            start_time, user_id, level, song_id, artist_id, session_id,
            location, user_agent
        )
        SELECT
            se.ts             AS start_time,
            se.userId         AS user_id,
            se.level          AS level,
            ss.song_id        AS song_id,
            ss.artist_id      AS artist_id,
            se.sessionId      AS session_id,
            se.location       AS location,
            se.userAgent      AS user_agent
        FROM staging_events se
        LEFT JOIN staging_songs ss
            ON (
                se.artist = ss.artist_name AND
                se.song   = ss.title       AND
                se.length = ss.duration
            )
        WHERE se.page = 'NextSong'
    """)

    # Use a 2nd WHERE to ensure user_id is distinct
    # https://knowledge.udacity.com/questions/276119
    # This is the proper replacement for ON CONFLICT.
    users_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            se.userId           AS user_id,
            se.firstName        AS first_name,
            se.lastName         AS last_name,
            se.gender           AS gender,
            se.level            AS level
        FROM staging_events se
        WHERE
            se.page = 'NextSong' AND
            se.userId IS NOT NULL AND
            user_id NOT IN (SELECT DISTINCT user_id FROM users)
    """)

    songs_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            ss.song_id         AS song_id,
            ss.title           AS title,
            ss.artist_id       AS artist_id,
            ss.year            AS year,
            ss.duration        AS duration
        FROM staging_songs ss
        WHERE ss.song_id IS NOT NULL
    """)

    artists_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            ss.artist_id          AS artist_id,
            ss.artist_name        AS name,
            ss.artist_location    AS location,
            ss.artist_latitude    AS latitude,
            ss.artist_longitude   AS longitude
        FROM staging_songs ss
        WHERE ss.artist_id IS NOT NULL
    """)

    # References regarding converting Epoch Time in milliseconds
    #   into Redshift SQL Timestamp (that EXTRACT function knows)
    # https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
    # https://knowledge.udacity.com/questions/64294
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
        SELECT DISTINCT
            se.ts AS start_time,
            EXTRACT(HOUR FROM se.ts) AS hour,
            EXTRACT(DAY FROM se.ts) AS day,
            EXTRACT(WEEK FROM se.ts) AS week,
            EXTRACT(MONTH FROM se.ts) AS month,
            EXTRACT(YEAR FROM se.ts) AS year,
            EXTRACT(DOW FROM se.ts) AS weekday
        FROM (
            SELECT DISTINCT ts
            FROM staging_events
            WHERE page = 'NextSong' AND ts IS NOT NULL
        ) se
    """)
    