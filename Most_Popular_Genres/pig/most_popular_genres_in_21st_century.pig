/* Load the user_song_listen_count triplet */
user_song_listencount_triplet = LOAD 's3://user_triplets.txt' USING PigStorage() AS (user_id:chararray, song_id:chararray, listen_count:int);

/* Remove extra attributes from user_song_listen_count triplet*/
song_listen_count_pairs = FOREACH user_song_listencount_triplet GENERATE song_id, listen_count;


/* Load the artist_genre pairs */
artist_genre_pairs = LOAD 's3://artist_tags.txt' USING PigStorage() AS (artist_id:chararray, genre:chararray);


/* Load the song artist pairs */
track_metadata = LOAD 's3://unique_tracks.txt' USING PigStorage();

/* Remove extra attributes from projected_song_artist_pairs*/
song_artist_pairs = FOREACH track_metadata GENERATE $2 AS song_id, $4 AS artist_id, (int)$10 AS year;

/* Apply filter for year*/
year_filtered_song_artist_pairs = FILTER song_artist_pairs BY year >= 2000 AND year <= 2010;


/* Join song_artist_pairs and artist_genre_pairs based on artist id to build song_genre_pairs*/
song_genre_pairs = JOIN year_filtered_song_artist_pairs BY (artist_id), artist_genre_pairs BY (artist_id);

/* Join song_genre_pairs and song_listen_count_pairs based on artist id to build genre_listen_count_pairs*/
joined_genre_listen_count_pairs = JOIN song_listen_count_pairs BY (song_id), song_genre_pairs BY (song_id);

/* Remove extra attributes from projected_song_artist_pairs*/
genre_listen_count_pairs = FOREACH joined_genre_listen_count_pairs GENERATE genre, listen_count;


/* Compute  listen count for each individual genre */
grouped_genre_listen_count_pairs = GROUP genre_listen_count_pairs BY genre;
aggregated_genre_listen_count_pairs = FOREACH grouped_genre_listen_count_pairs GENERATE group AS genre, SUM(genre_listen_count_pairs.listen_count) AS listen_count;

ordered_genre_listen_count_pairs = ORDER aggregated_genre_listen_count_pairs BY listen_count DESC;

limited_genre_listen_count_pairs = LIMIT ordered_genre_listen_count_pairs 10;

/* Store the output of limited_genre_listen_count_pairs */
store limited_genre_listen_count_pairs into 's3://most_popular_genres_in_21st_century_using_pig';
