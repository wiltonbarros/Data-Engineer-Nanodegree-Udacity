# DROP TABLES

songplay_table_drop = "DROP TABLE if exists songplays"
user_table_drop = "DROP TABLE if exists users"
song_table_drop = "DROP TABLE if exists songs"
artist_table_drop = "DROP TABLE if exists artists"
time_table_drop = "DROP TABLE if exists time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (songplay_id SERIAL primary key, 
start_time bigint NOT NULL, user_id integer NOT NULL, 
level varchar, song_id varchar, artist_id varchar, 
session_id integer, location varchar, user_agent varchar); 
""")

user_table_create = ("""
CREATE TABLE users (user_id int primary key, first_name varchar, 
last_name varchar, gender char(1), level varchar);
""")

song_table_create = ("""
CREATE TABLE songs (song_id varchar primary key, title varchar, 
artist_id varchar, year int, 
duration numeric);
""")

artist_table_create = ("""
CREATE TABLE artists(artist_id varchar primary key, name varchar, 
location varchar, latitude numeric, 
longitude numeric);
""")

time_table_create = ("""
CREATE TABLE time(start_time time, hour int, day int, week int, 
month int, year int, day_week int );
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT into songplays (start_time, user_id, level, song_id, artist_id, 
session_id, location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level; 
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration )
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude ) 
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, day_week ) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT B.artist_id, A.song_id  FROM SONGS A LEFT JOIN  ARTISTS B ON A.artist_id = B.artist_id  
where a.title = %s and b.name =%s and a.duration = %s 
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]