package com.airline.flight.delay.populate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import au.com.bytecode.opencsv.CSVParser;

/*
 * Mapper class for reading the input flight data
 * Writes each record 1-to-1 to an HBase table
 */
public class HPopulate {
  
	private static final String FLIGHT_INFO_TABLE_NAME = "FlightInfo";
	private static final String FLIGHT_COLOUMN_FAMILY = "FlightFamily";
	
	/*
	 * Map each input record to a row in HBASE table
	 */
	public static class AirlineInfoMapper extends Mapper<Object, Text, ImmutableBytesWritable, Writable> {
		
		/*************** Indexes of relevant data in csv file *****************/
		private static final int YEAR_INDEX = 0;
		private static final int MONTH_INDEX = 2;
		private static final int DAY_INDEX = 3;
		private static final int UNIQUE_CARRIER_INDEX = 6;
		private static final int FLIGHT_NUM_INDEX = 10;
		private static final int ORIGIN_INDEX = 11;
		private static final int DEST_INDEX = 17;
		private static final int ARRIVAL_DELAY_INDEX = 37;
		private static final int FLIGHT_CANCELLED_INDEX = 41;
		
		private static final String FLIGHT_YEAR_COLOUMN = "Year";
		private static final String FLIGHT_DELAY_COLOUMN = "ArrDelayMinutes";
		private static final String FLIGHT_CANCELLED_COLOUMN = "Cancelled";
		
		private Configuration config;
	    private HTable flightInfoTable;
	    private CSVParser parser;
	    
	    /*
	     * Setup initial configuration for each map task
	     */
	    protected void setup(Context context) throws IOException {
	    	this.config = HBaseConfiguration.create();
	    	this.parser = new CSVParser();
	    	this.flightInfoTable = new HTable(config, FLIGHT_INFO_TABLE_NAME);
	    	
	    	//Disable flush, setup buffer to write as bulk
	    	flightInfoTable.setWriteBufferSize(51200);
	    	flightInfoTable.setAutoFlush(false);
	    }
	
	    /*
		 * Key : Offset in input file to read the data from
		 * Value : UTF-8 string representation of data at given offset
		 */
	    public void map(Object offset, Text value, Context context)
	    		throws IOException, InterruptedException {
	    	
			String[] flightDetails = this.parser.parseLine(value.toString());
			StringBuilder rowKey = new StringBuilder();
	
			if (flightDetails != null && flightDetails.length > 0) {
				//Build a unique key for table
				rowKey.append(flightDetails[UNIQUE_CARRIER_INDEX]).append(";");
				rowKey.append(flightDetails[YEAR_INDEX]).append(";");
				rowKey.append(flightDetails[MONTH_INDEX]).append(";");
				rowKey.append(flightDetails[DAY_INDEX]).append(";");
				rowKey.append(flightDetails[FLIGHT_NUM_INDEX]).append(";");
				rowKey.append(flightDetails[ORIGIN_INDEX]).append(";");
				rowKey.append(flightDetails[DEST_INDEX]);
				
				//Put all the flight data into a hbase row
				Put row = new Put(Bytes.toBytes(rowKey.toString()));
				for (int count = 0; count < flightDetails.length; count++) {
					row.add(FLIGHT_COLOUMN_FAMILY.getBytes(), new String("C" + count).getBytes(), 
							flightDetails[count].getBytes());
				}
				
				//Put the row in the hbase table
				flightInfoTable.put(row);
			}
	    }
	    
	    protected void cleanup(Context context) throws IOException, InterruptedException{
	    	flightInfoTable.close();
	    }
	}
	
	/*
	 * Create the hbase table using HBaseAdmin
	 */
	public static void createFlightInfoTable() throws IOException, ZooKeeperConnectionException {

		// Instantiating configuration class
		Configuration hBaseConfig = HBaseConfiguration.create();

		// Instantiating HbaseAdmin class
		HBaseAdmin hBaseAdmin = new HBaseAdmin(hBaseConfig);

		// Instantiating table descriptor class
        HTableDescriptor hBaseTableDescriptor = new HTableDescriptor(FLIGHT_INFO_TABLE_NAME);

        // Adding column families to table descriptor
        hBaseTableDescriptor.addFamily(new HColumnDescriptor(FLIGHT_COLOUMN_FAMILY));

        //Execute the table through admin
        if(hBaseAdmin.tableExists(FLIGHT_INFO_TABLE_NAME))
        {
        	hBaseAdmin.disableTable(FLIGHT_INFO_TABLE_NAME);
        	hBaseAdmin.deleteTable(FLIGHT_INFO_TABLE_NAME);
        }
        hBaseAdmin.createTable(hBaseTableDescriptor);
        hBaseAdmin.close();
	}

	
	public static void main(String[] args) throws Exception {
		
		Configuration computeDelayConf = new Configuration();
		computeDelayConf.set(TableOutputFormat.OUTPUT_TABLE, FLIGHT_INFO_TABLE_NAME);
		
		String[] otherArgs = new GenericOptionsParser(computeDelayConf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AirlineAverageFlightDelay <in> <out>");
			System.exit(2);
		}
		
		createFlightInfoTable();
		
		Job computeDelayJob = new Job(computeDelayConf, "Compute Flight Delays");
		computeDelayJob.setJarByClass(HPopulate.class);
		computeDelayJob.setMapperClass(AirlineInfoMapper.class);
		computeDelayJob.setOutputKeyClass(TableOutputFormat.class);
		computeDelayJob.setOutputValueClass(Text.class);
		computeDelayJob.setNumReduceTasks(0);

		FileInputFormat.addInputPath(computeDelayJob, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(computeDelayJob, new Path(otherArgs[1]));
		System.exit(computeDelayJob.waitForCompletion(true) ? 0 : 1);
	}
}
