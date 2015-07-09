## Synopsis

Goal is	to compute the	average	delay for all two-leg flights between an single pair of origin and destination in a given date range  (Plain Map Reduce vs Apache Pig)
## Description

Origin, destination and date range are specified as constants within the program.  
  
**A valid two leg flight(F1:F2) is one that satisfies following rules**:  
  
  * F1 has	given origin and	some destination X that	is different	from	given destination .  
  * F2	originates	from	that	airport	X	where	F1	ended;	its	destination	is	given destination.  
  * F1	and	F2	have	the	same	flight	date. (Use	the	FlightDate attribute.)  
  * The	departure	time	of	F2	is later	than	the	arrival	time	of	F1.  
  * Neither	of	the	two	flights	was	cancelled or	diverted.  

**Plain Map Reduce (Java):**  
Using flight date as the key; applying projections and removing irrelevant data at the earliest to improve join performance.  
  
**Apache Pig**  
  * JoinFirstBothInRange - First self join, then apply date range condition on both flight legs  
  * JoinFirstFlight1InRange - First self join, then apply date range condition on first flight leg  
  * FilterFirstAverageFlightDelay - Apply date range condition on flights, then self join  
  
Purpose for multiple pig programs was to understand the pig optimizer; It automatically optimizes query plan, thus all the above pig programs have
similar performance.  

## Testing  

Tested on airline dataset using Aamazon EMR