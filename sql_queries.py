import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS "staging_events" (
                                artist VARCHAR,
                                auth VARCHAR,
                                firstName VARCHAR,
                                gender VARCHAR,
                                itemInSession INTEGER,
                                lastName VARCHAR,
                                length DECIMAL,
                                level VARCHAR,
                                location VARCHAR,
                                method VARCHAR,
                                page VARCHAR,
                                registration DECIMAL,
                                sessionId BIGINT,
                                song VARCHAR,
                                status DECIMAL,
                                ts TIMESTAMP,
                                userAgent VARCHAR,
                                userId VARCHAR);""")
   
staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS "staging_songs" (
                                artist_id VARCHAR,
                                artist_latitude DECIMAL,
                                artist_location VARCHAR,
                                artist_longitude DECIMAL,
                                artist_name VARCHAR,
                                duration DECIMAL,
                                num_songs BIGINT,
                                song_id VARCHAR,
                                title VARCHAR,
                                year BIGINT);""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id INTEGER GENERATED ALWAYS AS IDENTITY,
                                start_time TIMESTAMP NOT NULL,
                                FOREIGN KEY (start_time) REFERENCES time(start_time), 
                                user_id VARCHAR NOT NULL, 
                                FOREIGN KEY (user_id) REFERENCES users(user_id), 
                                level VARCHAR, 
                                song_id VARCHAR,
                                FOREIGN KEY (song_id) REFERENCES songs(song_id), 
                                artist_id VARCHAR ,
                                FOREIGN KEY (artist_id) REFERENCES artists(artist_id), 
                                session_id BIGINT, 
                                location VARCHAR, 
                                user_agent VARCHAR);""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                                user_id VARCHAR PRIMARY KEY, 
                                first_name VARCHAR, 
                                last_name VARCHAR, 
                                gender VARCHAR, 
                                level VARCHAR);""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                                song_id VARCHAR PRIMARY KEY, 
                                title VARCHAR, 
                                artist_id VARCHAR, 
                                year INTEGER, 
                                duration DECIMAL);""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
                                artist_id VARCHAR PRIMARY KEY, 
                                name VARCHAR, 
                                location VARCHAR, 
                                latitude DECIMAL, 
                                longitude DECIMAL);""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                                start_time TIMESTAMP NOT NULL PRIMARY KEY, 
                                hour INTEGER, 
                                day INTEGER, 
                                week INTEGER, 
                                month INTEGER, 
                                year INTEGER, 
                                weekday INTEGER);""")

# STAGING TABLES
staging_events_copy = (f"""
                        COPY staging_events FROM '{LOG_DATA}'
                        CREDENTIALS 'aws_iam_role={ARN}'
                        FORMAT AS JSON '{LOG_JSONPATH}'
                        compupdate off 
                        TIMEFORMAT as 'epochmillisecs'
                        region 'us-west-2';""")


staging_songs_copy = (f"""
                        COPY staging_songs FROM '{SONG_DATA}'
                        CREDENTIALS 'aws_iam_role={ARN}'
                        JSON 'auto'
                        compupdate off 
                        region 'us-west-2';""")

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        SELECT DISTINCT ev.ts,
                        ev.userId,
                        ev.level,
                        sn.song_id,
                        sn.artist_id,
                        ev.sessionId,
                        ev.location,
                        ev.userAgent
                        FROM staging_events AS ev
                        JOIN staging_songs AS sn ON ev.artist = sn.artist_name
                        WHERE ev.page = 'NextSong' """)

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT ev.userId,
                        ev.firstName,
                        ev.lastName,
                        ev.gender,
                        ev.level
                        FROM staging_events AS ev
                        WHERE ev.page = 'NextSong'AND ev.userId IS NOT NULL""")


song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT sn.song_id,
                        sn.title,
                        sn.artist_id,
                        sn.year,
                        sn.duration
                        FROM staging_songs AS sn
                        WHERE sn.song_id IS NOT NULL""")

                      
artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                        SELECT DISTINCT sn.artist_id,
                        sn.artist_name,
                        sn.artist_location,
                        sn.artist_latitude,
                        sn.artist_longitude
                        FROM staging_songs AS sn
                        WHERE sn.artist_id IS NOT NULL""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT ev.ts,
                        EXTRACT(hour FROM ev.ts),
                        EXTRACT(day FROM ev.ts),
                        EXTRACT(week FROM ev.ts),
                        EXTRACT(month FROM ev.ts),
                        EXTRACT(year FROM ev.ts),
                        EXTRACT(weekday FROM ev.ts)
                        FROM staging_events ev
                        WHERE ev.page = 'NextSong' AND ev.ts IS NOT NULL""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, song_table_create, time_table_create, user_table_create, artist_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
