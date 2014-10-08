package be.ae.hdp.mr.examples.wiki.totalorder;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<Text, Text, Text, Text>{
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key, value);
	}
}
