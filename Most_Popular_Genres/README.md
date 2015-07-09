## Synopsis

Determine "Most popular music genre's in 21st century" using a very simple genre recognition on Million Song Dataset(MSD).  

There are 2 main goals of this project:  
1. Learn similar technologies Apache Pig and Apache Hive and compare their performance for an individual task.  
2. Extract the "Top ten most popular genre's in 21st century" using the Million Song Dataset and The Echo Nest.  


## Description

To get the top ten genres I use the following 3 datasets:  

1. Echo Nest Taste Profile   
	a. The dataset contains real user - play counts from undisclosed partners for individual songs, with all songs already matched to the MSD  
	b. Metadata about the subset  
		i. 1,019,318 unique users  
		ii. 384,546 unique MSD songs  
		iii. 48,373,586 user - song - play count triplets  
	c. Format : Tab delimited plain text format  
	d. Sample data  
		|              Echo Nest User ID           |       Song ID      | Play count |  
		|------------------------------------------|--------------------|------------|  
		| b80344d063b5ccb3212f76538f3d9e43d87dca9e | SOAKIMP12A8C130995 |     1      |  
		| b80344d063b5ccb3212f76538f3d9e43d87dca9e | SOAPDEY12A81C210A9 |     5      |  

2. Echo Nest Artist Genres  
	a. The dataset contains genres associated with an individual artist, as artist-genre pairs, with all artists already matched with artist for songs contained in MSD  
	b. Metadata about the subset  
		i. 44,745 unique artists  
		ii. 24,777 artist – term pairs  
	c. Format sqlite database, preprocessed to Tab delimited plain text format  
	d. Sample data  
		|      Artist ID     | Genre |  
		|--------------------|-------|  
		| AR002UA1187B9A637D | Pop   |  
		| AR002UA1187B9A637D | Rock  |  
		| ARHFGKH1187B9A88D2 | Pop   |  

3. MSD Track metadata  
	a. Containing most metadata about each track in MSD  
	b. Metadata about set  
		i. 1 million songs / tracks  
		ii. Relevant fields song_id, artist_id, year  
	c. Format sqlite database, preprocessed to Tab delimited plain text format  
	d. Sample data  
		| ... |       Song ID      |      Artist ID     | Year | ... |  
		|-----|--------------------|--------------------|------|-----|  
		|     | SOAKIMP12A8C130995 | AR002UA1187B9A637D | 2004 |     |  
		|     | SOAPDEY12A81C210A9 | ARHFGKH1187B9A88D2 | 2010 |     |  

## Testing  

Scripts were tested on Amazon EMR instances. Setting up hive on Amazon EMR was a tricky task in itself!  

## Disclaimer  

This project provides a rank aggregate using a very simple genre recognition, which itsel is an oversimplified approximation of automatic tagging.  
The genre of song was determined by song artist's genre tags.  
While this is not directly applicable, but since the purpose was data preparation, querying and performance comparison between pig & hive, it was still used.   

## References  

[Million Song Dataset](http://labrosa.ee.columbia.edu/millionsong/)  
[The Echo Nest](http://the.echonest.com/)

