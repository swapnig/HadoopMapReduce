package com.airline.flight.delay.compute;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.airline.flight.comparator.CarrierIDMonthKeyPair;

public class HCompute {
	
	private static final String FLIGHT_INFO_TABLE_NAME = "FlightInfo";
	private static final String FLIGHT_COLOUMN_FAMILY = "FlightFamily";
	private static final String FLIGHT_YEAR_COLOUMN = "C0";
	private static final String FLIGHT_DELAY_COLOUMN = "C37";
	private static final String FLIGHT_CANCELLED_COLOUMN = "C41";
	
	private static final String YEAR_OF_INTEREST = "2008";
	private static final String NOT_CANCELLED_FLIGHT = "0.00";
	
	public static class HComputeAirlineMapper extends TableMapper<CarrierIDMonthKeyPair, Text> {
		
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			
			CarrierIDMonthKeyPair keyPair = new CarrierIDMonthKeyPair();
			Text airlineDelay = new Text();
			
			String[] rowKeyAttributes = new String(value.getRow()).split(";");
			keyPair.setCarrierID(new Text(rowKeyAttributes[0]));
			keyPair.setMonth(new Text(rowKeyAttributes[2]));
			byte[] delay = value.getValue(FLIGHT_COLOUMN_FAMILY.getBytes(), FLIGHT_DELAY_COLOUMN.getBytes());
			if(delay != null && delay != "".getBytes()){
				airlineDelay.set(new String(value.getValue(FLIGHT_COLOUMN_FAMILY.getBytes(), 
						FLIGHT_DELAY_COLOUMN.getBytes())));
				context.write(keyPair, airlineDelay);
			}
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
	public static class HComputeAirlineReducer extends Reducer<CarrierIDMonthKeyPair, Text, Text, Text> {
		
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
				if(!value.toString().isEmpty()){
					airlineTotalDelayforMonth += Double.parseDouble(value.toString());
				}
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
	
	public static FilterList setupHbaseFilters() {
		
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		SingleColumnValueFilter yearOfInterestFilter = 
				new SingleColumnValueFilter(FLIGHT_COLOUMN_FAMILY.getBytes(), FLIGHT_YEAR_COLOUMN.getBytes(), 
						CompareOp.EQUAL, YEAR_OF_INTEREST.getBytes());
		filterList.addFilter(yearOfInterestFilter);
		
		SingleColumnValueFilter cancelledFilter = 
				new SingleColumnValueFilter(FLIGHT_COLOUMN_FAMILY.getBytes(), FLIGHT_CANCELLED_COLOUMN.getBytes(), 
						CompareOp.EQUAL, NOT_CANCELLED_FLIGHT.getBytes());
		filterList.addFilter(cancelledFilter);
		
		return filterList;
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration computeDelayConf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(computeDelayConf, args).getRemainingArgs();
		
		if (otherArgs.length != 1) {
			System.err.println("Usage: AirlineAverageFlightDelay <out>");
			System.exit(2);
		}
		
		FilterList filterList = setupHbaseFilters();
		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.setCaching(700);
		scan.setFilter(filterList);
			
		Job computeDelayJob = new Job(computeDelayConf, "Compute Flight Delays");
		computeDelayJob.setJarByClass(HCompute.class);
		computeDelayJob.setMapperClass(HComputeAirlineMapper.class);
		computeDelayJob.setPartitionerClass(FlightDataPartitioner.class);
		computeDelayJob.setSortComparatorClass(CarrierIDMonthKeyComparator.class);
		computeDelayJob.setGroupingComparatorClass(CarrierIDGroupComparator.class);
		computeDelayJob.setReducerClass(HComputeAirlineReducer.class);
		computeDelayJob.setOutputKeyClass(CarrierIDMonthKeyPair.class);
		computeDelayJob.setOutputValueClass(Text.class);
		computeDelayJob.setNumReduceTasks(10);

		TableMapReduceUtil.initTableMapperJob(FLIGHT_INFO_TABLE_NAME, scan,
				HComputeAirlineMapper.class, CarrierIDMonthKeyPair.class, Text.class, computeDelayJob);
		FileOutputFormat.setOutputPath(computeDelayJob, new Path(otherArgs[0]));

		System.exit(computeDelayJob.waitForCompletion(true) ? 0 : 1);
	}

}
