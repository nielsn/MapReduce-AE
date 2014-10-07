package be.ae.hdp.mr.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends 
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	public WordCountReducer() {
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable one : values) {
			sum += one.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}