import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN="arn:aws:iam::900076018425:role/myRedshiftRole"

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS staging_events; "
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = " DROP TABLE IF EXISTS songplay;"
user_table_drop = " DROP TABLE IF EXISTS users; "
song_table_drop = " DROP TABLE IF EXISTS songs; "
artist_table_drop = " DROP TABLE IF EXISTS artists; "
time_table_drop = " DROP TABLE IF EXISTS time; "

# CREATE TABLES

staging_events_table_create= ("""
SET search_path TO udacity;
CREATE TABLE IF NOT EXISTS staging_events (
    artist           VARCHAR,
    auth             VARCHAR,
    firstName       VARCHAR,
    gender           CHAR(1),
    itemInSession  INTEGER,
    lastName        VARCHAR,
    length           FLOAT,
    level            VARCHAR,
    location         VARCHAR,
    method           VARCHAR,
    page             VARCHAR,
    registration     FLOAT,
    sessionId        INTEGER,
    song             VARCHAR,
    status           INTEGER,
    ts               TIMESTAMP,
    userAgent       VARCHAR,
    userId           INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,    
    title            VARCHAR,
    duration         FLOAT,
    year             INTEGER); 
    
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
     id              BIGINT identity(0, 1) primary key sortkey,
     start_time      TIMESTAMP,
     user_id         INTEGER, 
     level           VARCHAR,
     song_id         VARCHAR,
     artist_id       VARCHAR,
     session_id      INTEGER,
     location        VARCHAR,
     user_agent     VARCHAR)

""")



user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id          INTEGER primary key sortkey, 
    first_name       VARCHAR, 
    last_name        VARCHAR, 
    gender           CHAR(1), 
    level            VARCHAR)

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id         VARCHAR primary key, 
    title           VARCHAR, 
    artist_id       VARCHAR, 
    year            INTEGER, 
    duration        FLOAT)

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR primary key, 
    name            VARCHAR, 
    location        VARCHAR , 
    latitude        FLOAT, 
    longitude       FLOAT)

""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time       TIMESTAMP primary key, 
    hour             INTEGER, 
    day              INTEGER, 
    week             INTEGER, 
    month            INTEGER, 
    year             INTEGER, 
    weekday          INTEGER)

""")

# STAGING TABLES

staging_events_copy = ("""
CREATE SCHEMA IF NOT EXISTS udacity;
SET search_path TO udacity;

copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json {}
timeformat as 'epochmillisecs';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)


staging_songs_copy = ("""
CREATE SCHEMA IF NOT EXISTS udacity;
SET search_path TO udacity;

copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto'
timeformat as 'epochmillisecs';

""").format(SONG_DATA,DWH_ROLE_ARN)



# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

    select e.ts as start_time,
	   e.userid,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionid,
       e.location,
       e.useragent
               from udacity.staging_events e
                join udacity.staging_songs s on s.artist_name = e.artist and s.title = e.song
                where e.page ='NextSong'

""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    
    select distinct(userid) as userid, 	
			   firstname, 
               lastname, 
               gender, 
               level 
               from udacity.staging_events where userid is not null
               							   and page ='NextSong'
		group by userid, firstname, lastname, gender, level

""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    
    select distinct(song_id)as songid,
	   title,
       artist_id,
       year,
       duration
               from udacity.staging_songs
               		where artist_id is not null
                    and   song_id is not null
    
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    
    select distinct(artist_id) as artist_id, 
	   artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
               from udacity.staging_songs
               		where artist_id is not null
    

""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    
    select ts as start_time,
	   extract(hour from ts)as hour,
       extract(day from ts)as day,
       extract(week from ts)as week,
       extract(month from ts)as month,
       extract(year from ts)as year,
       extract(dayofweek from ts)as weekday       
               from udacity.staging_events
""")

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



## References

"""
https://www.sisense.com/blog/double-your-redshift-performance-with-the-right-sortkeys-and-distkeys/#:~:text=A%20table's%20distkey%20is%20the,it's%20sorted%20within%20each%20node.

https://docs.aws.amazon.com/pt_br/redshift/latest/dg/r_CREATE_TABLE_examples.html

https://stackoverflow.com/questions/28496065/epoch-to-timeformat-yyyy-mm-dd-hhmiss-while-redshift-copy

https://docs.aws.amazon.com/pt_br/redshift/latest/dg/r_EXTRACT_function.html

"""