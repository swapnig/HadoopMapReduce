## Synopsis

Determine "Most popular music genre's in 21st century" using a very simple genre recognition on Million Song Dataset(MSD).  

There are 2 main goals of this project:  
* Learn similar technologies Apache Pig and Apache Hive and compare their performance for an individual task.  
* Extract the "Top ten most popular genre's in 21st century" using the Million Song Dataset and The Echo Nest.  


## Description

To get the top ten genres I use the following 3 datasets:  
  
* ** Echo Nest Taste Profile**   
  * The dataset contains real user - play counts from undisclosed partners for individual songs, with all songs already matched to the MSD  
  * Metadata about the subset  
	  * 1,019,318 unique users  
	  * 384,546 unique MSD songs  
	  * 48,373,586 user - song - play count triplets  
  * Format : Tab delimited plain text format  
  * Sample data  
		|              Echo Nest User ID           |       Song ID      | Play count |  
		|------------------------------------------|--------------------|------------|  
		| b80344d063b5ccb3212f76538f3d9e43d87dca9e | SOAKIMP12A8C130995 |     1      |  
		| b80344d063b5ccb3212f76538f3d9e43d87dca9e | SOAPDEY12A81C210A9 |     5      |  
  
* ** Echo Nest Artist Genres**  
  * The dataset contains genres associated with an individual artist, as artist-genre pairs, with all artists already matched with artist for songs contained in MSD  
  * Metadata about the subset  
	  * 44,745 unique artists  
	  * 24,777 artist – term pairs  
  * Format sqlite database, preprocessed to Tab delimited plain text format  
  * Sample data  
		|      Artist ID     | Genre |  
		|--------------------|-------|  
		| AR002UA1187B9A637D | Pop   |  
		| AR002UA1187B9A637D | Rock  |  
		| ARHFGKH1187B9A88D2 | Pop   |  
  
* ** MSD Track metadata**  
  * Containing most metadata about each track in MSD  
  * Metadata about set  
	  * 1 million songs / tracks  
	  * Relevant fields song_id, artist_id, year  
  * Format sqlite database, preprocessed to Tab delimited plain text format  
  * Sample data  
		| ... |       Song ID      |      Artist ID     | Year | ... |  
		|-----|--------------------|--------------------|------|-----|  
		|     | SOAKIMP12A8C130995 | AR002UA1187B9A637D | 2004 |     |  
		|     | SOAPDEY12A81C210A9 | ARHFGKH1187B9A88D2 | 2010 |     |  
  
## Testing  

Tested on million song dataset using Aamazon EMR Setting up hive on Amazon EMR was a tricky task in itself!  

## Disclaimer  

This project provides a rank aggregate using a very simple genre recognition, which itsel is an oversimplified approximation of automatic tagging.  
The genre of song was determined by song artist's genre tags.  
While this is not directly applicable, but since the purpose was data preparation, querying and performance comparison between pig & hive, it was still used  

## References  

[Million Song Dataset](http://labrosa.ee.columbia.edu/millionsong/)  
[The Echo Nest](http://the.echonest.com/)
