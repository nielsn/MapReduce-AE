package be.ae.hdp.mr.examples.wiki.partitioning;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
	
public class PartitioningMapper extends Mapper<LongWritable, WikipediaPage, IntWritable, Text>{
	private static final Pattern TIMESTAMP = Pattern.compile("<timestamp>.*?T");
	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"yyyy-MM-dd");
	
	@SuppressWarnings("deprecation")
	@Override
	public void map(LongWritable key, WikipediaPage page, Context context) throws IOException, InterruptedException{
		String xml = page.getRawXML();
		
		Matcher m = TIMESTAMP.matcher(xml);
		if(!m.find()) return;
		
		Date date = new Date();		
		try {
			date = frmt.parse(xml.substring(m.start(), m.end()-1).replaceAll("<.*?>", ""));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	 	 	
		
		context.write(new IntWritable(date.getYear() + 1900), new Text(page.getTitle()));
	}
}
