package be.ae.hdp.mr.examples.wiki.binning;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class BinningMapper extends Mapper<LongWritable, WikipediaPage, Text, NullWritable>{

	private static final Pattern CATEGORY = Pattern.compile("\\[\\[Category:.*?\\]\\]");
	private MultipleOutputs<Text, NullWritable> mos = null;
	
	@Override
	public void setup(Context context){
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}
	
	@Override
	public void map(LongWritable key, WikipediaPage page, Context context) throws IOException, InterruptedException{
		Set<String> categories = getCategories(page);
		
		if(categories.contains("USA") || categories.contains("United States") || categories.contains("America") || categories.contains("American")){
			mos.write("bins", new Text(page.getTitle()), NullWritable.get(), "category-US");
		}
		
		if(categories.contains("Russia") || categories.contains("Russian")){
			mos.write("bins", new Text(page.getTitle()), NullWritable.get(), "category-RU");
		}
		
		if(categories.contains("Greece") || categories.contains("Greek")){
			mos.write("bins", new Text(page.getTitle()), NullWritable.get(), "category-GR");
		}
	}

	private Set<String> getCategories(WikipediaPage page) {
		Matcher m = CATEGORY.matcher(page.getRawXML());
		
		Set<String> categories = new HashSet<String>();
		while(m.find()){
			categories.addAll(Arrays.asList(page.getRawXML().substring(m.start(), m.end())
										   .replaceAll("\\[\\[Category:", "")
										   .replaceAll("\\]\\]", "")
										   .split("[\\W\\s]+")));
		}
		return categories;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
