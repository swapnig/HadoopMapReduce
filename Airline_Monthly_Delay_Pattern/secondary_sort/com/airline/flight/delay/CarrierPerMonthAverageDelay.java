package com.airline.flight.delay;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.airline.flight.comparator.CarrierIDMonthKeyPair;

import au.com.bytecode.opencsv.CSVParser;

public class CarrierPerMonthAverageDelay {

	/*
	 * Mapper class for reading the input flight data
	 * Applies projection and selection while reading the input data
	 */
	public static class FlightDataMapper extends Mapper<Object, Text, CarrierIDMonthKeyPair, Text> {
		
		/*************** Indexes of relevant data in csv file *****************/
		private static final int YEAR_INDEX = 0;
		private static final int MONTH_INDEX = 2;
		private static final int UNIQUE_CARRIER_INDEX = 6;
		private static final int ARRIVAL_DELAY_INDEX = 37;
		private static final int FLIGHT_CANCELLED_INDEX = 41;
		
		/*************** Problem statement data *****************/
		private static double CANCELED = 1.00;
		private static int YEAR = 2008;
		
		//Initialize the parser for the input CSV file
		private CSVParser parser = new CSVParser();

		/*
		 * Key : Offset in input file to read the data from
		 * Value : UTF-8 string representation of data at given offset
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//Read the flight records as tokens
			String[] flightDetails = parser.parseLine(value.toString());			

			if (isValidFlight(flightDetails)) {
				
				//Create a composite key(secondary sort) for flight data
				CarrierIDMonthKeyPair CarrierIDMonthKeyPair = 
						new CarrierIDMonthKeyPair(flightDetails[UNIQUE_CARRIER_INDEX], flightDetails[MONTH_INDEX]);
				
				//Initialize and text for flight delay
				Text flightDelay = new Text();
				flightDelay.set(flightDetails[ARRIVAL_DELAY_INDEX].getBytes());
				
				//Emit the record
				context.write(CarrierIDMonthKeyPair, flightDelay);
			}
		}
		
		/*
		 * Verify whether the given flight is valid, 
		 * Ignoring flights that are missing one of the attributes needed for the computation
		 * Also enforce the selection conditions for year and cancelled flights
		 */
		public boolean isValidFlight(String[] flightDetails) {
			return ( !( "".equals(flightDetails[MONTH_INDEX]) || "".equals(flightDetails[UNIQUE_CARRIER_INDEX]) ||
					    "".equals(flightDetails[ARRIVAL_DELAY_INDEX])) &&
					 ( !("".equals(flightDetails[FLIGHT_CANCELLED_INDEX])) 
							 && CANCELED != Double.parseDouble(flightDetails[FLIGHT_CANCELLED_INDEX])) &&
				 	 ( !("".equals(flightDetails[YEAR_INDEX]))  
				 			 && YEAR == Integer.parseInt(flightDetails[YEAR_INDEX])));
		}
	}
	
	/*
	 * Custom partitioner for flight data,
	 * Uses the hash code of the underlying key to determine the partition
	 */
	public static class FlightDataPartitioner extends Partitioner<CarrierIDMonthKeyPair, Text> {

		public int getPartition(CarrierIDMonthKeyPair key, Text value, int numReduceTasks) {
			//Multiply by 127 to perform some mixing
			return Math.abs(key.getCarrierID().hashCode() * 127) % numReduceTasks;
		}
	}
	
	/*
	 * Custom comparator that controls how the keys are sorted before they are passed to the Reducer
	 * Here we use the composite key of Unique carrier id and month of travel
	 * It first sorts on the basis of carrier id and then sorts in increasing order of month of journey
	 */
	public static class CarrierIDMonthKeyComparator extends WritableComparator {
	    protected CarrierIDMonthKeyComparator() {
	        super(CarrierIDMonthKeyPair.class, true);
	    }   
	    
	    @SuppressWarnings("rawtypes")
		@Override
	    public int compare(WritableComparable writable1, WritableComparable writable2) {
	    	CarrierIDMonthKeyPair flight1 = (CarrierIDMonthKeyPair)writable1;
	    	CarrierIDMonthKeyPair flight2 = (CarrierIDMonthKeyPair)writable2;
	         
	        return flight1.compareCarrierIDAndMonth(flight2);
	    }
	}
	
	/*
	 * Custom comparator that controls which keys are grouped together for a single call to reduce
	 * Here we use the first half of composite key, Unique carrier id
	 * It groups all the records with the same carrier id irrespective of their month of travel
	 */
	public static class CarrierIDGroupComparator extends WritableComparator {
	    protected CarrierIDGroupComparator() {
	        super(CarrierIDMonthKeyPair.class, true);
	    }   
	    
	    @SuppressWarnings("rawtypes")
		@Override
	    public int compare(WritableComparable writable1, WritableComparable writable2) {
	    	CarrierIDMonthKeyPair flight1 = (CarrierIDMonthKeyPair)writable1;
	    	CarrierIDMonthKeyPair flight2 = (CarrierIDMonthKeyPair)writable2;
	         
	        return flight1.compareCarrierIDOnly(flight2);
	    }
	}

	/*
	 * Reducer for computing the average delay by month for each carrier
	 */
	public static class AirlineReducer extends Reducer<CarrierIDMonthKeyPair, Text, Text, Text> {
		
		/*
		 * Key : Composite key(UniqueCarrierID, Month)
		 * Values : All the records with for an individual unique carrier, sorted in increasing order of month
		 */
		public void reduce(CarrierIDMonthKeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//Initialize the counters for average delay computation
			int currentMonthNum = 1;
			int totalFlightsByMonth = 0;
			double airlineTotalDelayforMonth = 0;
			
			//Initialize the variable to store the output text
			StringBuffer airlineDelayInformationByMonth = new StringBuffer(key.getCarrierID().toString());

			// For each flight of a given carrier compute average delay by month
			for (Text value : values) {
				int keyMonthNum = Integer.parseInt(key.getMonth().toString());
				
				//If a new month has been found, add its average delay to the output text
				if(currentMonthNum != keyMonthNum) {
					int airlineAverageDelayforMonth = (int)(Math.ceil(airlineTotalDelayforMonth / totalFlightsByMonth));
					airlineDelayInformationByMonth.append(", (" + currentMonthNum + ", " + airlineAverageDelayforMonth + ")");
					
					//Increment the month counter by 1 to handle the case of a no flight during amonth
					currentMonthNum++;
					
					//Reset counters used in the average delay computation
					totalFlightsByMonth = 0;
					airlineTotalDelayforMonth = 0;
				}
				//Increment the counters for average delay computation of current month
				totalFlightsByMonth++;
				airlineTotalDelayforMonth += Double.parseDouble(value.toString());
			}
			
			//Write average delay for the last month with flight, as well as any remaining months
			while(currentMonthNum <= 12){
				int averageDelay = (int)(Math.ceil(airlineTotalDelayforMonth / totalFlightsByMonth));
				airlineDelayInformationByMonth.append(", (" + currentMonthNum + ", " + averageDelay + ")");
				airlineTotalDelayforMonth = 0;
				totalFlightsByMonth = 0;
				currentMonthNum++;
			}
			
			//Emit the average delay for each airline per month
			context.write(new Text(""), new Text(airlineDelayInformationByMonth.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration computeDelayConf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(computeDelayConf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: AirlineAverageFlightDelay <in> <out>");
			System.exit(2);
		}
			
		Job computeDelayJob = new Job(computeDelayConf, "Compute Flight Delays");
		computeDelayJob.setJarByClass(CarrierPerMonthAverageDelay.class);
		
		//Setup map reduce classes
		computeDelayJob.setMapperClass(FlightDataMapper.class);
		computeDelayJob.setPartitionerClass(FlightDataPartitioner.class);
		computeDelayJob.setSortComparatorClass(CarrierIDMonthKeyComparator.class);
		computeDelayJob.setGroupingComparatorClass(CarrierIDGroupComparator.class);
		computeDelayJob.setReducerClass(AirlineReducer.class);
		
		//Setup output classes
		computeDelayJob.setOutputKeyClass(CarrierIDMonthKeyPair.class);
		computeDelayJob.setOutputValueClass(Text.class);
		
		//Set number of reduce tasks
		computeDelayJob.setNumReduceTasks(10);

		FileInputFormat.addInputPath(computeDelayJob, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(computeDelayJob, new Path(otherArgs[1]));

		System.exit(computeDelayJob.waitForCompletion(true) ? 0 : 1);
	}
}
