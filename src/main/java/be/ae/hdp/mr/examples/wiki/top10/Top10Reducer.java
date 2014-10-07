package be.ae.hdp.mr.examples.wiki.top10;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10Reducer extends Reducer<NullWritable, Text, Text, IntWritable>{
	
	private static final int LIMIT = 10;
	private TreeMap<Integer, Text> countToPageMap = new TreeMap<Integer, Text>();
	
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text text : values){
			int separatorIndex = text.toString().indexOf(":::");
			Text docId = new Text(text.toString().substring(0, separatorIndex));
			Integer linkCount = Integer.parseInt(text.toString().substring(separatorIndex + 3));
			
			countToPageMap.put(linkCount, docId);
			
			if(countToPageMap.size() > LIMIT)
				countToPageMap.remove(countToPageMap.firstKey());
		}
		
		for(Integer count : countToPageMap.descendingMap().keySet())
			context.write(countToPageMap.get(count), new IntWritable(count));
	}		
}
