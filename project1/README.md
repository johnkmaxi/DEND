# Purpose

Sparkify is a music streaming service. Sparkify intends to leverage user data to understand their users, their platform, and enhance the quality of their service. The analytical goals of Sparkify are to understand user behavior and the context of the that behavior. To acheive that goal, Sparkify has captured logs of user activity on the platform and logs of songs that are played on the platform. This data collection lends itself to understanding what songs users are listening to, and how they interact with the platform during those songs.

# Database Schema and ETL Pipeline

The primary goal of Sparify analytics is user understanding. Therefore, I have designed a database designed for easy reporting using a star schema. The central fact table of the star schema is the songplays table. Dimension tables related to this fact table include users, songs, artists, and time tables. These tables capture additional details to provide additional context to the central songplays fact table. The table names and data contained in each are listed below. The songplays fact table provides information about each song a particular user listens, where they were at the time, what method they used to access Sparkify, and whether they are paid or free users.

The designed ETL pipeline reads json files containing information about a particular song and json files capturing user interaction with Sparkify. The song log files contain the information to populate both the songs table and the artists table. These json files are opened, the data is parsed to acquire the song or artist information, and then loaded into the corresponding table. The user log files form the basis for the songplays, users, and time tables. From these log files, the timestamp associated with the user activity is converted to a human-readable time stamp and several characteristics of the time stamp are saved in the time table: hour, day, week, month, year, weekday (which day of the week). Second, the information about the user is captured and loaded into the users table. Finally, information about each songplay is loaded into the songplays fact table.

The original plan to capture song_id and artist_id was scrapped do to a mismatch between what songs are available in the log files, and what songs occur in the user log files. This discrepancy resulted in all but one row in the songplays table have values of None for song_id and artist_id. I substituted the actual song title and artist name for these fields at this time. In the future, when a larger library of song files is available, we can switch to using the song and artist ids.

The schema supports the understanding of user behavior: How many users per month? How many songs does a user listen to per session? Do paid users listen more than free users? What is the most popular artist? Most popular song? When is usage of Sparkify the greatest?

# Usage

To create the tables using the defined schema and ETL process:
1. Make sure the data folder containing the log_data and song_data are in your working directory
2. In the terminal, run create_tables.py to reset and create the database
3. In the terminal, run etl.py to extract, transform, and load the json-stored data into the database.
4. Analysis - your preference, jupyter notebook, interactive python session in the terminal, or postgresql console. Can insert or reference the queries in sql_queries.py to get you started!

### songplays
songplay_id SERIAL PRIMARY KEY
start_time timestamptz
user_id varchar
level varchar
song_id varchar
artist_id varchar
session_id int
location varchar
user_agent varchar

### users
user_id varchar PRIMARY KEY
first_name varchar
last_name varchar
gender varchar
level varchar

In the users table, whenever there is a conflict on the primary key, we update first_name last_name, gender, and level in case the user has changed their name, gender preference, or level.

### songs
song_id varchar PRIMARY KEY
title varchar
artist_id varchar
year int
duration float

### artists
artist_id varchar PRIMARY KEY
name varchar
location varchar
latitude float
longitude float

### time
start_time timestamp with time zone PRIMARY KEY
hour int
day int
week int
month int
year int
weekday int

# Example queries and results

The following queries are available in sql_queries.py for analysts to use in their analysis.

How many users per month? 
monthly_users = """SELECT time.month, time.year, COUNT( DISTINCT user_id)
                            FROM songplays
                            JOIN time
                            ON songplays.start_time = time.start_time
                            GROUP BY time.month, time.year"""

Sparkify currently has only one month of data indicating 96 users in November 2011.

How many songs does a user listen to per session?

listens_per_session = """SELECT oq.user_id, AVG(sq."Listens Per Session")
                        FROM songplays oq
                            JOIN (SELECT user_id, session_id, COUNT(songplay_id) "Listens Per Session"
                            FROM songplays
                            GROUP BY user_id, session_id
                            ORDER BY "Listens Per Session" DESC) sq
                            ON oq.user_id = sq.user_id
                        GROUP BY oq.user_id
                        ORDER BY AVG(sq."Listens Per Session") DESC"""
                        
This query indicates that the top user listens to 43.5 songs per session, on average.

Do paid users listen more than free users? 

listens_by_user_level = """SELECT oq.level, AVG(sq."Listens Per Session")
                        FROM songplays oq
                            JOIN (SELECT user_id, session_id, COUNT(songplay_id) "Listens Per Session"
                            FROM songplays
                            GROUP BY user_id, session_id
                            ORDER BY "Listens Per Session" DESC) sq
                            ON oq.user_id = sq.user_id
                        GROUP BY oq.level
                        ORDER BY AVG(sq."Listens Per Session") DESC"""

The results show that paid users listen to almost 6x more songs per session.

What is the most popular artist? 

popular_artists = """SELECT artist_id, COUNT(songplay_id) "Artist Count"
                    FROM songplays
                    GROUP BY artist_id
                    ORDER BY "Artist Count" DESC
                    LIMIT 10"""

The top 3 most popular artists are Coldplay, Kings Of Leon, and Dwight Yoakam.
                    
Most popular song? When is usage of Sparkify the greatest?

popular_songs = """SELECT song_id, COUNT(songplay_id) "Song Count"
                    FROM songplays
                    GROUP BY song_id
                    ORDER BY "Song Count" DESC
                    LIMIT 10"""

The top 3 most popular songs are You're The One, Undo, and Revelry.