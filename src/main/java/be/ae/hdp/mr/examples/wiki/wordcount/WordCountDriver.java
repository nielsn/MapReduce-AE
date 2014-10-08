package be.ae.hdp.mr.examples.wiki.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import be.ae.hdp.mr.examples.common.Utils;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;

public class WordCountDriver extends Configured implements Tool{
	private static final String appName = "WikiWordCount";
	private static Logger logger = Logger.getLogger(WordCountDriver.class);

	public static void main(String[] args) throws Exception {
		// This is the root logger provided by log4j
		logger = Logger.getRootLogger();
		logger.setLevel(Level.INFO);

		// Define log pattern layout
		PatternLayout layout = new PatternLayout(
				"%d{ISO8601} [%t] %-5p %c %x - %m%n");

		// Add console appender to root logger
		logger.addAppender(new ConsoleAppender(layout));

		try {
			logger.addAppender(new FileAppender(layout, Utils
					.getUniqueLogFolder(appName, "output") + "/" + "log.txt"));
		} catch (IOException e) {
			logger.error("Could not find logging location");
			e.printStackTrace();
		}

		ToolRunner.run(new WordCountDriver(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(WordCountDriver.class);
		job.setJobName(appName);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,
				new Path(Utils.getUniqueOutputFolder(appName, args[1])));
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 2;
	}
	
	

}
