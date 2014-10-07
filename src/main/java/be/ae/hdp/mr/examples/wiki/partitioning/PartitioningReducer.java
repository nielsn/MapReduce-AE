package be.ae.hdp.mr.examples.wiki.partitioning;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartitioningReducer extends Reducer<IntWritable, Text, Text, IntWritable>{

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text t : values)
			context.write(t, key);
	}
}
