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
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(songplay_id integer IDENTITY(0,1) PRIMARY KEY NOT NULL
,artist varchar
,auth varchar
,firstName varchar
,gender varchar
,itemInSession integer
,lastName varchar
,length float
,level varchar
,location varchar
,method varchar
,page varchar
,registration float
,sessionId integer
,song varchar
,status integer
,ts timestamp
,userAgent varchar
,userId integer)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(song_id varchar PRIMARY KEY NOT NULL
,title varchar
,artist_id varchar
,artist_name varchar
,artist_latitude float
,artist_longitude float
,artist_location varchar
,duration float
,year int
,num_songs int)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(songplay_id integer IDENTITY(0,1) PRIMARY KEY UNIQUE NOT NULL
,start_time timestamptz NOT NULL
,user_id int NOT NULL
,level varchar NOT NULL
,song_id varchar NOT NULL
,artist_id varchar NOT NULL
,session_id int NOT NULL
,location varchar
,user_agent varchar)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id int PRIMARY KEY NOT NULL
,first_name varchar
,last_name varchar
,gender varchar
,level varchar NOT NULL)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(song_id varchar PRIMARY KEY NOT NULL
,title varchar NOT NULL
,artist_id varchar NOT NULL
,year int
,duration float)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id varchar PRIMARY KEY NOT NULL
,name varchar NOT NULL
,location varchar
,latitude float
,longitude float)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times
(start_time timestamptz PRIMARY KEY NOT NULL
,hour int
,day int
,week int
,month int
,year int
,weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
iam_role '{}'
REGION 'us-west-2' 
FORMAT json '{}'
TIMEFORMAT AS 'epochmillisecs'
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
iam_role '{}'
REGION 'us-west-2' 
FORMAT json 'auto';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays
(start_time, user_id, level, song_id,
artist_id, session_id, location, user_agent)
select staging_events.ts
       ,staging_events.userId
       ,staging_events.level
       ,staging_songs.song_id
       ,staging_songs.artist_id
       ,staging_events.sessionId
       ,staging_events.location
       ,staging_events.userAgent
  from staging_events 
       join staging_songs
       on staging_events.song = staging_songs.title
       and staging_events.artist = staging_songs.artist_name
 where staging_events.page = 'NextSong'; 
""")

user_table_insert = ("""
insert into users
select distinct staging_events.userId
       ,staging_events.firstName
       ,staging_events.lastName
       ,staging_events.gender
       ,staging_events.level
  from staging_events
 where staging_events.page = 'NextSong';
""")

song_table_insert = ("""
insert into songs
select distinct staging_songs.song_id
       ,staging_songs.title
       ,staging_songs.artist_id
       ,staging_songs.year
       ,staging_songs.duration
  from staging_songs
""")

artist_table_insert = ("""
insert into artists
select distinct staging_songs.artist_id
       ,staging_songs.artist_name
       ,staging_songs.artist_location
       ,staging_songs.artist_latitude
       ,staging_songs.artist_longitude
  from staging_songs
""")

time_table_insert = ("""
insert into times
select distinct staging_events.ts
       ,date_part(h, staging_events.ts) 
       ,date_part(d, staging_events.ts)
       ,date_part(w, staging_events.ts)
       ,date_part(mon, staging_events.ts)
       ,date_part(y, staging_events.ts)
       ,date_part(dayofweek, staging_events.ts)
  from staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, 
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, 
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
