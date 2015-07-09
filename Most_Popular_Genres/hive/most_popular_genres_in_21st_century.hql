
DROP TABLE if EXISTS user_song_listencount_triplet_subset;
DROP TABLE if EXISTS song_artist_pairs_subset;
DROP TABLE if EXISTS artist_tag_pairs_subset;
DROP TABLE if EXISTS tag_listen_pairs;


CREATE EXTERNAL TABLE user_song_listencount_triplet_subset (
	user_id STRING, 
	song_id STRING, 
	listen_count INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 's3n://user_triplets/';

CREATE EXTERNAL TABLE artist_tag_pairs_subset (
	artist_id STRING, 
	tag_name STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 's3n://artist_tags/';

CREATE EXTERNAL TABLE song_artist_pairs_subset (
	track_id STRING, 
	song_title STRING, 
	song_id STRING, 
	release STRING,
	artist_id STRING,
	artist_mbid STRING,
	artist_name STRING,
	duration STRING,
	artist_familiarity STRING,
	artist_hotness STRING,
	year STRING,
	digital_id STRING,
	shs_1 STRING,
	shs_2 STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 's3n://unique_tracks/';

CREATE EXTERNAL TABLE most_popular_genres_in_21st_century (
	tag STRING,
	listen_count INT) LOCATION 's3n://most_popular_genres_in_21st_century_using_hive/';

INSERT INTO TABLE most_popular_genres_in_21st_century
SELECT artist_tag_pairs_subset.tag_name, SUM(user_song_listencount_triplet_subset.listen_count) as tag_listen_count
FROM song_artist_pairs_subset JOIN artist_tag_pairs_subset ON (song_artist_pairs_subset.artist_id = artist_tag_pairs_subset.artist_id) JOIN user_song_listencount_triplet_subset ON (user_song_listencount_triplet_subset.song_id = song_artist_pairs_subset.song_id)
WHERE song_artist_pairs_subset.year >= 2000
GROUP BY artist_tag_pairs_subset.tag_name
ORDER BY tag_listen_count DESC
LIMIT 10;
