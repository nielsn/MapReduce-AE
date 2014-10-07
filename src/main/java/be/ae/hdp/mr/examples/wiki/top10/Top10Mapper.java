package be.ae.hdp.mr.examples.wiki.top10;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class Top10Mapper extends Mapper<LongWritable, WikipediaPage, NullWritable, Text>{
	private static final int LIMIT = 10;
	private TreeMap<Integer, Text> countToPageMap = new TreeMap<Integer, Text>();
	
	@Override
	public void map(LongWritable key, WikipediaPage page, Context context){
		int linkCount = page.extractLinks().size();
		String title = page.getTitle();
		
		countToPageMap.put(new Integer(linkCount), new Text(title + ":::" + linkCount));
		
		if(countToPageMap.size() > LIMIT){
			countToPageMap.remove(countToPageMap.firstKey());
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		for(Text text : countToPageMap.values())
			context.write(NullWritable.get(), text);
	}
}
