package be.ae.hdp.mr.examples.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable{

	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new String[0];
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
