/* Use CSV loader for parsing csv data */
REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

/* Setting number of reducer tasks to 10 */
SET default_parallel 10;

/* Load  the  flights  file  as  Flights1 */
Flights1 = LOAD 's3://swap-hw3/input/data.csv' using CSVLoader;

/* Load  the  flights  file  again  as  Flights2 */
Flights2 = LOAD 's3://swap-hw3/input/data.csv' using CSVLoader;

/* Remove  as  many  records using problem statement excluding the condition that the flight date has to be between June 2007 and May 2008 */
/* Relevant headers $0 : Month, $2 : Month, $11 : Origin, $17 : Destination, $41 : Cancelled, $43 : Diverted */
validFlights1 = FILTER Flights1 BY ($41 != 1 AND $43 != 1 AND $11 == 'ORD' AND $17 != 'JFK');
validFlights2 = FILTER Flights2 BY ($41 != 1 AND $43 != 1 AND $11 != 'ORD' AND $17 == 'JFK');

/* Remove extra attributes */
validFlights1Data = FOREACH validFlights1 GENERATE ToDate($5,'yyyy-MM-dd') AS DATE1, $11 AS ORG1, $17 AS DEST1, (int)$24 AS DEPT1, (int)$35 AS ARR1, (float)$37 AS ArrDelayTime1, (int)$0 AS YEAR1, (int)$2 AS MONTH1;
validFlights2Data = FOREACH validFlights2 GENERATE ToDate($5,'yyyy-MM-dd') AS DATE2, $11 AS ORG2, $17 AS DEST2, (int)$24 AS DEPT2, (int)$35 AS ARR2, (float)$37 AS ArrDelayTime2, (int)$0 AS YEAR2, (int)$2 AS MONTH2;

/* Join  Flights1  and  Flights2,  using  the  condition  that  the  destination  airport  in  Flights1  matches  
the  origin  in  Flights2  and  that  both  have  the  same  flight  date*/
joinedTwoLegFlights = JOIN validFlights1Data BY (DEST1, DATE1), validFlights2Data BY (ORG2, DATE2);

/* Filter  out  those  join  tuples  where  the  departure  time  in  Flights2  is  not  after  the  arrival  time  in  Flights1 */
validTwoLegFlights = FILTER joinedTwoLegFlights BY DEPT2 > ARR1;

/* Filter  out  those  tuples  whose  both the flight  dates  is  not  between  June  2007  and May 2008 */
validTwoLegFlightsWithinDate = FILTER validTwoLegFlights BY (((YEAR1 == 2007 AND MONTH1 >= 6) OR (YEAR1 == 2008 AND MONTH1 <= 5)) AND ((YEAR2 == 2007 AND MONTH2 >= 6) OR (YEAR2 == 2008 AND MONTH2 <= 5)));

/* Compute  the  delay  over  all  the  tuples  produced  in  the  previous step */
delayValidTwoLegFlights = FOREACH validTwoLegFlightsWithinDate GENERATE ArrDelayTime1 + ArrDelayTime2 AS delaySum;

/* Compute  the  average  delay  over  all  the  tuples  produced  in  the  previous step */
groupedValidTwoLegFlights = GROUP delayValidTwoLegFlights ALL;
flightDelayAverage = FOREACH groupedValidTwoLegFlights GENERATE AVG(delayValidTwoLegFlights.delaySum);

/* Store the output of Filter first */
store flightDelayAverage into 's3://swap-hw3/pig-execution/join_first_both_output';
