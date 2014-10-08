package be.ae.hdp.mr.examples.wiki.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class WordCountMapper extends Mapper<LongWritable, WikipediaPage, Text, IntWritable>{
	
	private IntWritable one = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, WikipediaPage page, Context context) throws IOException, InterruptedException{
		String text = page.getDisplayContent();
		
		for(String word : text.trim().toLowerCase().replace('_', ' ').split("[\\W\\s]+")){
			context.write(new Text(word), one);
		}
	}
}
