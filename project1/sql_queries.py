# TABLES

tables = ['songplays','users','songs','artists','time']
#songplay_table_drop = ""
#user_table_drop = ""
#song_table_drop = ""
#artist_table_drop = ""
#time_table_drop = ""

# CREATE TABLES

columns = {'songplays': """(songplay_id SERIAL PRIMARY KEY NOT NULL
                ,start_time timestamptz NOT NULL
                ,user_id varchar NOT NULL
                ,level varchar NOT NULL
                ,song_id varchar NOT NULL
                ,artist_id varchar NOT NULL
                ,session_id int NOT NULL
                ,location varchar
                ,user_agent varchar)""",
          'users': """(user_id varchar PRIMARY KEY NOT NULL
                ,first_name varchar
                ,last_name varchar
                ,gender varchar
                ,level varchar NOT NULL)""",
          'songs': """(song_id varchar PRIMARY KEY NOT NULL
                ,title varchar NOT NULL
                ,artist_id varchar NOT NULL
                ,year int
                ,duration float)""",
          'artists': """(artist_id varchar PRIMARY KEY NOT NULL
                ,name varchar NOT NULL
                ,location varchar
                ,latitude float
                ,longitude float)""",
          'time': """(start_time timestamp with time zone PRIMARY KEY NOT NULL
                ,hour int
                ,day int
                ,week int
                ,month int
                ,year int
                ,weekday int)"""}

#songplay_table_create = ("""
#""")

#user_table_create = ("""
#""")

#song_table_create = ("""
#""")

#artist_table_create = ("""
#""")

#time_table_create = ("""
#""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(
                                start_time
                                ,user_id
                                ,level
                                ,song_id
                                ,artist_id
                                ,session_id
                                ,location
                                ,user_agent)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users(
                            user_id
                            ,first_name
                            ,last_name
                            ,gender
                            ,level)
                        VALUES(%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id)
                        DO UPDATE SET (first_name, last_name, gender, level) = (EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.gender, EXCLUDED.level)
""")

song_table_insert = ("""INSERT INTO songs(
                                    song_id
                                    ,title
                                    ,artist_id
                                    ,year
                                    ,duration)
                                VALUES(%s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists(
                                artist_id
                                ,name
                                ,location
                                ,latitude
                                ,longitude)
                            VALUES(%s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING

""")


time_table_insert = ("""INSERT INTO time(
                            start_time
                            ,hour
                            ,day
                            ,week
                            ,month
                            ,year
                            ,weekday)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
""")

# FIND SONGS
# Implement the song_select query in sql_queries.py to find the song ID and artist ID based on the title, artist name, and duration of a song.
song_select = ("""SELECT
                    -- song_id
                    songs.song_id
                    -- artist_id
                   ,songs.artist_id
                FROM
                    songs
                    JOIN artists
                        ON songs.artist_id = artists.artist_id
                WHERE
                    songs.title = %s
                    AND
                    artists.name = %s
                    AND
                    songs.duration = %s;
""")

monthly_users = """SELECT time.month, time.year, COUNT( DISTINCT user_id)
                     FROM songplays
                          JOIN time
                          ON songplays.start_time = time.start_time
                    GROUP BY time.month, time.year"""

listens_per_session = """SELECT oq.user_id
                                ,AVG(sq."Listens Per Session")
                           FROM songplays oq
                                JOIN (SELECT user_id
                                             ,session_id
                                             ,COUNT(songplay_id) "Listens Per Session"
                                        FROM songplays
                                       GROUP BY user_id
                                             ,session_id
                                       ORDER BY "Listens Per Session" DESC) sq
                                ON oq.user_id = sq.user_id
                          GROUP BY oq.user_id
                          ORDER BY AVG(sq."Listens Per Session") DESC"""

listens_by_user_level = """SELECT oq.level, AVG(sq."Listens Per Session")
                        FROM songplays oq
                            JOIN (SELECT user_id, session_id, COUNT(songplay_id) "Listens Per Session"
                            FROM songplays
                            GROUP BY user_id, session_id
                            ORDER BY "Listens Per Session" DESC) sq
                            ON oq.user_id = sq.user_id
                        GROUP BY oq.level
                        ORDER BY AVG(sq."Listens Per Session") DESC"""

popular_artists = """SELECT artist_id, COUNT(songplay_id) "Artist Count"
                    FROM songplays
                    GROUP BY artist_id
                    ORDER BY "Artist Count" DESC
                    LIMIT 10"""

popular_songs = """SELECT song_id, COUNT(songplay_id) "Song Count"
                    FROM songplays
                    GROUP BY song_id
                    ORDER BY "Song Count" DESC
                    LIMIT 10"""

weekly_usage = """SELECT time.weekday, COUNT(songplay_id)
                FROM songplays
                JOIN time
                ON songplays.start_time = time.start_time
                GROUP BY time.weekday
                ORDER BY time.weekday"""

# QUERY LISTS

#create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
#drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
