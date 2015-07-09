/* Use CSV loader for parsing csv data */
REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

/* Setting number of reducer tasks to 10 */
SET default_parallel 10;

/* Load  the  flights  file  as  Flights */
Flights = LOAD 's3://swap-hw3/input/data.csv' using CSVLoader;

/* Remove  as  many  records using problem statement as well as the condition that the flight date has to be between June 2007 and May 2008 */
/* Relevant headers $0 : Month, $2 : Month, $11 : Origin, $17 : Destination, $41 : Cancelled, $43 : Diverted */
validFlights1 = FILTER Flights BY ($41 != 1 AND $43 != 1 AND (($0 == 2007 AND $2 >= 6) OR ($0 == 2008 AND $2 <= 5)) AND $11 == 'ORD' AND $17 != 'JFK');
validFlights2 = FILTER Flights BY ($41 != 1 AND $43 != 1 AND (($0 == 2007 AND $2 >= 6) OR ($0 == 2008 AND $2 <= 5)) AND $11 != 'ORD' AND $17 == 'JFK');

/* Remove extra attributes */
validFlights1Data = FOREACH validFlights1 GENERATE $5 AS DATE1, $11 AS ORG1, $17 AS DEST1, (int)$24 AS DEPT1, (int)$35 AS ARR1, (float)$37 AS ArrDelayTime1;
validFlights2Data = FOREACH validFlights2 GENERATE $5 AS DATE2, $11 AS ORG2, $17 AS DEST2, (int)$24 AS DEPT2, (int)$35 AS ARR2, (float)$37 AS ArrDelayTime2;

/* Join  Flights1  and  Flights2,  using  the  condition  that  the  destination  airport  in  Flights1  matches  
the  origin  in  Flights2  and  that  both  have  the  same  flight  date*/
joinedTwoLegFlights = JOIN validFlights1Data BY (DEST1, DATE1), validFlights2Data BY (ORG2, DATE2);

/* Filter  out  those  join  tuples  where  the  departure  time  in  Flights2  is  not  after  the  arrival  time  in  Flights1 */
validTwoLegFlights = FILTER joinedTwoLegFlights BY DEPT2 > ARR1;

/* Compute  the  delay  over  all  the  tuples  produced  in  the  previous step */
delayValidTwoLegFlights = FOREACH validTwoLegFlights GENERATE ArrDelayTime1 + ArrDelayTime2 AS delaySum;

/* Compute  the  average  delay  over  all  the  tuples  produced  in  the  previous step */
groupedValidTwoLegFlights = GROUP delayValidTwoLegFlights ALL;
flightDelayAverage = FOREACH groupedValidTwoLegFlights GENERATE AVG(delayValidTwoLegFlights.delaySum);

/* Store the output of Filter first */
store flightDelayAverage into 's3://swap-hw3/pig-execution/filter_first_output_single_load';
