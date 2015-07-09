package com.airline.flight.comparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class CarrierIDMonthKeyPair implements WritableComparable{
	
	Text carrierID;
	Text month;
	
	/*
	 * Default constructor
	 */
	public CarrierIDMonthKeyPair() {
		this.carrierID = new Text();
    	this.month = new Text();
	}
	
	/*
	 * Constructor expecting carrierID and month
	 */
	public CarrierIDMonthKeyPair(String carrierID, String month) {
		this.carrierID = new Text(carrierID.getBytes());
		this.month = new Text(month.getBytes());
	}

	@Override
	public int compareTo(Object object) {
		CarrierIDMonthKeyPair airlineMonthPair = (CarrierIDMonthKeyPair) object;
		return getCarrierID().compareTo(airlineMonthPair.getCarrierID());
	}
	
	/*
	 * Used by grouping comparator, only uses carrier ID for comparison between 2 objects
	 */
	public int compareCarrierIDOnly(Object object) {
		CarrierIDMonthKeyPair airlineMonthPair = (CarrierIDMonthKeyPair) object;
		return getCarrierID().compareTo(airlineMonthPair.getCarrierID());
	}
	
	/*
	 * Used by sort(key) comparator, first sorts by carrier id
	 * Then sorts in increasing order of month
	 */
	public int compareCarrierIDAndMonth(Object object) {
		CarrierIDMonthKeyPair airlineMonthPair = (CarrierIDMonthKeyPair) object;
		int airlineComparison = getCarrierID().compareTo(airlineMonthPair.getCarrierID());
		
        if (0 != airlineComparison) {
        	return airlineComparison;
        }
        Integer month1 = Integer.parseInt(getMonth().toString());
        Integer month2 = Integer.parseInt(airlineMonthPair.getMonth().toString());
        return month1.compareTo(month2);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.carrierID.readFields(input);
        this.month.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		this.carrierID.write(output);
    	this.month.write(output);
	}

    public boolean equals(Object object) {
    	CarrierIDMonthKeyPair flight = (CarrierIDMonthKeyPair) object;
            return this.carrierID.equals(flight.getCarrierID());
    }

	public Text getCarrierID() {
		return carrierID;
	}

	public void setCarrierID(Text carrierID) {
		this.carrierID = carrierID;
	}

	public Text getMonth() {
		return month;
	}

	public void setMonth(Text month) {
		this.month = month;
	}

}

