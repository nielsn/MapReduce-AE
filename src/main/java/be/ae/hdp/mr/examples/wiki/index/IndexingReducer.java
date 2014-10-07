package be.ae.hdp.mr.examples.wiki.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexingReducer extends Reducer<Text, LongWritable, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		Set<Long> res = new HashSet<Long>();
		
		for(LongWritable value : values){
			if(!res.contains(value.get())){
				res.add(value.get());
			}
		}
		
		context.write(key, new Text(Arrays.toString(res.toArray())));
	}
}
