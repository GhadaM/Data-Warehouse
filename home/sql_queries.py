import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE","ARN")
LOG_JSONPATH =config.get("S3","LOG_JSONPATH")
# DROP TABLES

staging_events_table_drop = "Drop Table if exists staging_event"
staging_songs_table_drop = "Drop Table if exists staging_songs"
songplay_table_drop = "Drop Table if exists songplays cascade"
user_table_drop = "Drop Table if exists users"
song_table_drop = "Drop Table if exists songs"
artist_table_drop = "Drop Table if exists artists"
time_table_drop = "Drop Table if exists time"

# CREATE TABLES

staging_events_table_create = ("""
Create Table if not exists staging_event (
    artist        varchar, 
    auth          varchar,
    firstName     varchar,
    gender        varchar,
    itemInSession varchar,
    lastName      varchar,
    length        float,
    level         varchar,
    location      varchar,
    method        varchar,
    page          varchar,
    registration  varchar,
    sessionId     integer,
    song          varchar,
    status        integer,
    ts            BIGINT,
    userAgent     varchar,
    userId        varchar
)
""")

staging_songs_table_create = ("""
Create Table if not exists staging_songs (
    artist_id        varchar, 
    artist_latitude  float,
    artist_location  varchar(255),
    artist_longitude float,
    artist_name      varchar(255), 
    duration         float, 
    num_songs        varchar, 
    song_id          varchar , 
    title            varchar(255), 
    year             integer
)
""")

songplay_table_create = ("""
Create Table songplays (
    songplay_id integer IDENTITY(0,1) primary key sortkey, 
    start_time  varchar(25) not null, 
    user_id     integer not null, 
    level       varchar(10) not null, 
    song_id     varchar(50) not null distkey, 
    artist_id   varchar(50) not null, 
    session_id  integer not null, 
    location    varchar(255) not null, 
    user_agent  varchar(255) not null
)
""")

user_table_create = ("""
Create Table users (
    user_id     integer primary key sortkey, 
    first_name  varchar(50), 
    last_name   varchar(50), 
    gender      varchar(10), 
    level       varchar(10) not null
)
diststyle all;
""")

song_table_create = ("""
Create Table songs (
    song_id     varchar(50) primary key sortkey, 
    title       varchar(255) not null, 
    artist_id   varchar(50) not null, 
    year        integer     not null, 
    duration    float  
)
diststyle all;
""")

artist_table_create = ("""
Create Table artists (
    artist_id  varchar(50) primary key sortkey, 
    name       varchar(255) not null, 
    location   varchar(255), 
    latitude   float, 
    longitude  float
)
diststyle all;
""")

time_table_create = ("""
Create Table time (
    start_time  varchar(25) primary key sortkey, 
    hour        integer not null, 
    day         integer not null, 
    week        integer not null, 
    month       integer not null,
    year        integer not null,
    weekday     integer not null
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_event from 's3://udacity-dend/log_data' 
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format(ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data' 
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
    Insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select TO_CHAR(timestamp 'epoch' + se.ts/1000 * interval '1 second' , 'YYYY-MM-DD HH:MM:SS') as start_time,
    se.userId::numeric::int as user_id, 
    se.level as level, 
    ss.song_id as song_id, 
    ss.artist_id as artist_id, 
    se.sessionId as session_id, 
    se.location as location, 
    se.userAgent as user_agent
 
    From staging_event se
    Join staging_songs ss
    On se.artist = ss.artist_name
    And se.song = ss.title
    And se.length = ss.duration
    Where se.page = 'NextSong'
""")

user_table_insert = (""" 
    Insert into users (user_id, first_name, last_name, gender, level)
    Select distinct se.userId::numeric::int as user_id, 
    se.firstName as first_name, 
    se.lastName as last_name, 
    se.gender as gender, 
    se.level as level
    
    From staging_event se
    Where se.page = 'NextSong'
""")

song_table_insert = ("""
    Insert into songs (song_id, title, artist_id, year, duration)
    Select ss.song_id as song_id, 
    ss.title as title, 
    ss.artist_id as artist_id, 
    ss.year as year, 
    ss.duration as duration
    
    From staging_songs ss
""")

artist_table_insert = ("""
    Insert into artists (artist_id, name, location, latitude, longitude)
    Select distinct ss.artist_id as artist_id, 
    ss.artist_name as name, 
    ss.artist_location as location, 
    ss.artist_latitude as latitude, 
    ss.artist_longitude as longitude
    
    From staging_songs ss
""")

time_table_insert = ("""
    Insert into time (start_time, hour, day, week, month ,year, weekday)
    select distinct TO_CHAR(timestamp 'epoch' + se.ts/1000 * interval '1 second' , 'YYYY-MM-DD HH:MM:SS') as start_time,
    date_part('hour', start_time::timestamp) as hour,
    date_part('day', start_time::timestamp) as day,
    date_part('week', start_time::timestamp) as week,
    date_part('month', start_time::timestamp) as month,
    date_part('year', start_time::timestamp) as year,
    date_part('dow', start_time::timestamp) as weekday
    
    From staging_event se
    Where se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]