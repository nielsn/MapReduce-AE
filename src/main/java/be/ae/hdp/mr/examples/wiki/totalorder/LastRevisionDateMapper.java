package be.ae.hdp.mr.examples.wiki.totalorder;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class LastRevisionDateMapper extends Mapper<LongWritable, WikipediaPage, Text, Text>{
	private static final Pattern TIMESTAMP = Pattern.compile("<timestamp>.*?</timestamp>");

	@Override
	public void map(LongWritable key, WikipediaPage page, Context context) throws IOException, InterruptedException{
		String xml = page.getRawXML();
		
		Matcher m = TIMESTAMP.matcher(xml);
		if(!m.find()) return;	 	 	
		
		String date = xml.substring(m.start(), m.end()).replaceAll("<.*?>", "").replaceAll("</.*?>", "");
		context.write(new Text(date), new Text(page.getTitle()));
	}
}
