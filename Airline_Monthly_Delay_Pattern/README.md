## Synopsis

Goal is to compute the pattern of monthly delays for each airline (Apache Hbase vs Secondary Sort).  

More precisely, for each airline, the program produces an output line like this:  

AIR-A, (1, A1), (2, A2),…, (11, A11), (12, A12)  
AIR-B, (1, B1), (2, B2),…, (11, B11), (12, B12)  
  
Here AIR-A stands for an airline name. Pair (i, Ai) indicates the average delay Ai of airline AIR-A in month i of the year 2008. For example, (6,17) means
that AIR-A had an average delay of 17 minutes in June 2008. All such pairs are sorted in increasing order of month.


## Description

**Secondary Sort** : Uses value-to-key conversion design pattern to partition by airlineId and sort by month.  
  
**KeyComparator (keypair)** : Object with 2 attributes airlineId and month  
  
**Hbase :**  

  * H-POPULATE: This program reads records from the input file and writes each record 1-to-1 to an HBase table. All records are stored in the same table. For input record r, there is exactly one matching row r’ in that table. Also for n records in input file, HBase table contains the corresponding n rows with all the fields from the n input records.  
  
  * H-COMPUTE: This program reads from the HBase table to generate the desired output file.   
  
**Value-to-key conversion design pattern:**  
– To partition by X and then sort each X-group by Y, make (X, Y) the key  
– Define key comparator to order by composite key (X, Y)  
– Define partitioner and grouping comparator for (X,Y) to consider only X for partitioning and grouping  

## Testing  

Tested on airline dataset using Aamazon EMR

## References  

[Design Patterns](http://www.ccs.neu.edu/home/mirek/classes/2012-F-CS6240/Slides/4-DesignPatterns.pdf)  