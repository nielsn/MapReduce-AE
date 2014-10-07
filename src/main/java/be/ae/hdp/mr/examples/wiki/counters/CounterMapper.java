package be.ae.hdp.mr.examples.wiki.counters;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

public class CounterMapper extends
		Mapper<LongWritable, WikipediaPage, Text, IntWritable> {

	private static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, OTHER
	};

	@Override
	public void map(LongWritable key, WikipediaPage p, Context context)
			throws IOException, InterruptedException {
		context.getCounter(PageTypes.TOTAL).increment(1);
		if (p.isRedirect()) {
			context.getCounter(PageTypes.REDIRECT).increment(1);
		} else if (p.isDisambiguation()) {
			context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
		} else if (p.isEmpty()) {
			context.getCounter(PageTypes.EMPTY).increment(1);
		} else if (p.isArticle()) {
			context.getCounter(PageTypes.ARTICLE).increment(1);
			if (p.isStub()) {
				context.getCounter(PageTypes.STUB).increment(1);
			}
		} else {
			context.getCounter(PageTypes.OTHER).increment(1);
		}
	}
}
