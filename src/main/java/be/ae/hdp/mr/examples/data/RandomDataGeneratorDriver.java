package be.ae.hdp.mr.examples.data;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import be.ae.hdp.mr.examples.common.Utils;

public class RandomDataGeneratorDriver extends Configured implements Tool{

	private static final Logger logger = Logger.getLogger(RandomDataGeneratorDriver.class);
	
	public int run(String[] args) throws Exception {		
		logger.debug("Getting started");
		
		int numMapTasks = Integer.parseInt(args[0]);
		int numRecordsPerTask = Integer.parseInt(args[1]);
		
		Path wordlist = new Path(args[2]);
		Path outputDir = new Path(Utils.getUniqueOutputFolder(args[3]));
		
		Job job = new Job();
		job.setJarByClass(RandomDataGeneratorDriver.class);
		
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(RandomProductDataInputFormat.class);
		RandomProductDataInputFormat.setNumMapTasks(job, numMapTasks);
		RandomProductDataInputFormat.setNumRecordsPerMapTask(job, numRecordsPerTask);
		RandomProductDataInputFormat.setRandomWordList(job, wordlist);
		
		TextOutputFormat.setOutputPath(job, outputDir);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		logger.debug("Running job");
		System.exit(job.waitForCompletion(true)? 0 : 2);
		return job.waitForCompletion(true)? 0: 2;
	}
	
	public static void main(String[] args) throws Exception{
		//This is the root logger provided by log4j
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.INFO);
		 
		//Define log pattern layout
		PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");
		 
		//Add console appender to root logger
		rootLogger.addAppender(new ConsoleAppender(layout));
		
		
		RandomDataGeneratorDriver driver = new RandomDataGeneratorDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
