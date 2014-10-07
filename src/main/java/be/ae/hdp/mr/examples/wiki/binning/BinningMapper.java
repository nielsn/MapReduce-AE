package be.ae.hdp.mr.examples.wiki.binning;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class BinningMapper extends Mapper<LongWritable, WikipediaPage, Text, NullWritable>{

	@Override
	public void map(LongWritable key, WikipediaPage page, Context context){
		
	}
}
