package com.search.flights;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class TwoLegFlightsAverageDelay {

	private static String ORIGIN = "ORD";
	private static String DESTINATION = "JFK";

	public static class FlightDataMapper extends Mapper<Object, Text, Text, Text> {

		private Text flightDate = new Text();
		private Text flightDetailsText = new Text();
		
		/*************** Indexes of relevant data in csv file start *****************/
		private static int yearIndex = 0;
		private static int monthIndex = 2;
		private static int dateIndex = 5;
		private static int originIndex = 11;
		private static int destinationIndex = 17;
		private static int departureTimeIndex = 24;
		private static int arrivalTimeIndex = 35;
		private static int arrivalDelay = 37;
		private static int cancelledIndex = 41;
		private static int divertedIndex = 43;
		/*************** Indexes of relevant data in csv file end *****************/
		
		/*************** Problem statement data start *****************/
		
		private static double CANCELED = 1.00;
		private static double DIVERTED = 1.00;
		private static int START_YEAR = 2007;
		private static int END_YEAR = 2008;
		private static int START_MONTH_NUM = 6;
		private static int END_MONTH_NUM = 5;
		
		/*************** Problem statement data end *****************/

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			CSVParser parser = new CSVParser();
			String[] flightDetails = parser.parseLine(value.toString());			

			if (isValidFlight(flightDetails)) {
				//Set flight date as the intermediate key
				flightDate.set(flightDetails[dateIndex]);
				
				//Build relevant flight data
				String[] relevantFlightData = { 
						flightDetails[originIndex], flightDetails[destinationIndex], 
						flightDetails[departureTimeIndex], flightDetails[arrivalTimeIndex], 
						flightDetails[arrivalDelay]
				};
				//Emit relevant flight data with flight date as the key
				flightDetailsText.set(StringUtils.join(relevantFlightData, ","));
				context.write(flightDate, flightDetailsText);
			}
		}
		
		//Decide whether the flight with given details is a valid(relevant) flight
		public boolean isValidFlight(String[] flightDetails) {
			return (!(Double.parseDouble(flightDetails[cancelledIndex]) == CANCELED)
					&& !(Double.parseDouble(flightDetails[divertedIndex]) == DIVERTED)
					&& ((Integer.parseInt(flightDetails[yearIndex]) == START_YEAR 
							&& Integer.parseInt(flightDetails[monthIndex]) >= START_MONTH_NUM)
						|| (Integer.parseInt(flightDetails[yearIndex]) == END_YEAR 
							&& Integer.parseInt(flightDetails[monthIndex]) <= END_MONTH_NUM))
					&& ((flightDetails[originIndex].equals(ORIGIN) 
						|| flightDetails[destinationIndex].equals(DESTINATION))
							&& !(flightDetails[originIndex].equals(ORIGIN) 
								&& flightDetails[destinationIndex].equals(DESTINATION))));
		}
	}

	public static class FlightDataReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			List<String> firstLegList = new ArrayList<String>();
			List<String> secondLegList = new ArrayList<String>();

			//Build separate list for first leg and second leg of a flight
			for (Text value : values) {
				String[] flightData = value.toString().split(",");
				if (flightData[0].equals(ORIGIN)) {
					firstLegList.add(value.toString());
				} else if (flightData[1].equals(DESTINATION)) {
					secondLegList.add(value.toString());
				}
			}
			
			//For each first leg, loop to find a valid second leg
			for (String firstLeg : firstLegList) {
				String[] firstLegFields = firstLeg.toString().split(",");
				String firstLegDestination = firstLegFields[1];
				int firstLegArrivalTime = Integer.parseInt(firstLegFields[3]);
				
				//For each second leg, ascertain whether it is valid second leg for the current first leg
				for (String secondLeg : secondLegList) {
					String[] secondLegFields = secondLeg.toString().split(",");
					String secondLegOrigin = secondLegFields[0];
					int secondLegDepartureTime = Integer.parseInt(secondLegFields[2]);

					//Check whether the given set of first leg and second leg can together act as a two leg flight based on problem statement rules
					if (firstLegDestination.equals(secondLegOrigin) && (secondLegDepartureTime > firstLegArrivalTime)) {
						Double totalDelay = 
								Double.parseDouble(firstLegFields[4]) + Double.parseDouble(secondLegFields[4]);
						context.write(key, new Text(totalDelay.toString()));
					}
				}
			}
		}
	}
	
	public static class AverageDelayMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {

		private Text defaultKey = new Text("Default");

		/* Emit all the delays with the same key, so they are processes by same reduce call */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] flightDelays = value.toString().split("\t");
			context.write(defaultKey, new DoubleWritable(Double.parseDouble(flightDelays[1])));
		}
	}

	public static class AverageDelayReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		/* Compute average of all the flight delays and output a single record containing the average delay*/
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (DoubleWritable val : values) {
				count++;
				sum += val.get();
			}
			result.set((double)sum / count);
			context.write(new Text("Average Delay"), result);
		}
	}

	public static void main(String[] args) throws Exception {
		/* Setup first map reduce job */
		Configuration computeDelayConf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(computeDelayConf, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in>  <intermediate> <out>");
			System.exit(2);
		}

		Job computeDelayJob = new Job(computeDelayConf, "Compute Flight Delays");
		computeDelayJob.setJarByClass(TwoLegFlightsAverageDelay.class);
		computeDelayJob.setMapperClass(FlightDataMapper.class);
		computeDelayJob.setReducerClass(FlightDataReducer.class);
		computeDelayJob.setOutputKeyClass(Text.class);
		computeDelayJob.setOutputValueClass(Text.class);
		/* Set number of reduce tasks to 10*/
		computeDelayJob.setNumReduceTasks(10);

		FileInputFormat.addInputPath(computeDelayJob, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(computeDelayJob, new Path(otherArgs[1]));
		
		computeDelayJob.waitForCompletion(true);
		
		/* Setup second map reduce job*/
		Configuration computeAverageConf = new Configuration();

		Job computeAverageJob = new Job(computeAverageConf, "Compute Average FLight Delay");
		computeAverageJob.setJarByClass(TwoLegFlightsAverageDelay.class);
		computeAverageJob.setMapperClass(AverageDelayMapper.class);
		computeAverageJob.setReducerClass(AverageDelayReducer.class);
		computeAverageJob.setOutputKeyClass(Text.class);
		computeAverageJob.setOutputValueClass(DoubleWritable.class);
		computeAverageJob.setNumReduceTasks(1);

		/* Input file path to second map reduce job is same as the output file path of first map reduce job */
		FileInputFormat.addInputPath(computeAverageJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(computeAverageJob, new Path(otherArgs[2]));


		System.exit(computeAverageJob.waitForCompletion(true) ? 0 : 1);
	}
}
