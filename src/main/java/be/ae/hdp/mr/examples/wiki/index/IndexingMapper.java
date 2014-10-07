package be.ae.hdp.mr.examples.wiki.index;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class IndexingMapper extends Mapper<LongWritable, WikipediaPage, Text, LongWritable>{
	private Logger logger = Logger.getLogger(IndexingMapper.class);
	
	@Override
	public void map(LongWritable key, WikipediaPage p, Context context) throws IOException, InterruptedException {
		logger.debug(String.format("Started mapping - %s", p.getTitle()));
		
		String text = p.getDisplayContent();
		
		for(String word : text.trim().replace('_', ' ').split("[\\W\\s]+")){
			context.write(new Text(word.trim().toLowerCase()), key);
		}
		
		logger.debug(String.format("Finished mapping - %s", p.getTitle()));
	}
}
