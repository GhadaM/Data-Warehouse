# Project: Data Warehouse

In order to help Sparkify make use of the increasing amount od data, it is necessary to create an ETL pipeline that will load the data from the S3 bucket to a Redshift Cluster which will be a powerful solution to transform and analyse the raw data that can be used later by the analytics team.   

## Schema

##### Staging Tables

1. staging_event
- contains app activity logs
   * artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId       
2. staging_songs 
- contains metadata about songs including the artist info
   * artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year
   
##### Fact Table

3. songplays
- records in event data associated with song plays i.e. records with page NextSong
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent  
##### Dimension Tables
    
4. users
- users in the app
    * user_id, first_name, last_name, gender, level
5. songs
- songs in music database
    * song_id, title, artist_id, year, duration
6. artists
- artists in music database
    * artist_id, name, location, latitude, longitude
7. time
- timestamps of records in songplays broken down into specific units
    * start_time, hour, day, week, month, year, weekday
    
## ETL Pipeline
1. Extract the staging tables from S3 Bucket 
2. Transform ts(bigint) to start_time(datetime)
3. Load data from staging tables to fact and dimension tables

## Steps to follow and how to run
the jupyter notebook *steps_to_run.ipynb* contains the necessary steps to run create the environment and run the needed queries
1. we extract the configurations from dwh.cfg
2. we create the needed aws services  using boto3 library
3. we create the IAM role and attach policy to it
4. we create the Redshift cluster
5. then we open an incoming TCP port to access the cluster endpoint
6. we connect to the database 
7. we run the 3 script files 
   1. sql_queries.py: where we have the queries needed for the creation, deletion, loading from s3 bucket to staging tables and loading from staging tables to analytics tables
   2. create_tables.py: runs the create and delete queries
   3. etl.py: runs the copy queries to staging tables and loads the analytics tables 
8. after running some analytics queries
9. we delete the Redshift cluster and the IAM Role


## AWS Redshift Setup

- Cluster: 2 x dc2.large nodes 
- Location: eu-central-1

## Tables stats
* Row Count for each of the Tables:
  * staging_event: 8056
  * staging_songs: 14896
  * songplays: 319
  * users: 104
  * songs: 14896
  * artists: 10025
  * time: 5424


## Analytics Queries Examples

##### selecting the top 10 songs 

```
SELECT sp.song_id, s.title, s.year, s.duration, a.name, count(sp.song_id)
FROM Songplays sp
join Songs s
on s.song_id = sp.song_id
join artists a
on a.artist_id = sp.artist_id
Group by 1, 2, 3,4,5
order by count desc
limit 10
```
Result: 

| song_id	         |title                                                |year|duration|name                          |count|
| --- | ---| --- | ---| --- | --- |
| SOBONKR12A58A7A7E0 | You're The One                                      |1990|	  239.3073|Dwight Yoakam                 |	37|
| SOUNZHU12A8AE47481 | I CAN'T GET STARTED	                               |   0|	  497.13587|Ron Carter                    |	 9|
| SOHTKMO12AB01843B0 | Catch You Baby (Steve Pitron & Max Sanna Radio Edit)|   0|	  181.2109|Lonnie Gordon                 |	 9|
| SOULTKQ12AB018A183 | Nothin' On You [feat. Bruno Mars] (Album Version)   |2010|	  269.63546|B.o.B                         |	 8|
| SOLZOBD12AB0185720 | Hey Daddy (Daddy's Home)                            |2010|	  224.10404|Usher featuring Jermaine Dupri|	 6|
| SOARUPP12AB01842E0 | Up Up & Away                                        |2009|	  227.34322|Kid Cudi                      |	 5|
| SOTNHIP12AB0183131 | Make Her Say                                        |2009|	  237.76608|Kid Cudi                      |	 5|
| SOIOESO12A6D4F621D | Unwell (Album Version)                              |2003|	  229.14567|matchbox twenty               |	 4|
| SONQEYS12AF72AABC9 | Mr. Jones                                           |1991|	  272.79628|Counting Crows                |	 4|
| SOIZLKI12A6D4F7B61 | Supermassive Black Hole (Album Version)             |   0|	  209.34485|Muse                          |	 4|

##### Selecting Top user

```
select  u.first_name, u.last_name, count(*)
from Songplays sp
join users u
on u.user_id = sp.user_id
Group by 1,2
order by 3 desc
limit 1 
```
Result: 

|first_name|	last_name|	count|
| --- | ---| --- |
|Chloe	|Cuevas|	41|
