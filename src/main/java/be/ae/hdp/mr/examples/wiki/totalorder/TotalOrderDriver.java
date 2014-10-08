package be.ae.hdp.mr.examples.wiki.totalorder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import be.ae.hdp.mr.examples.common.Utils;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;

public class TotalOrderDriver extends Configured implements Tool {

	private static final String appName = "WikiTotalOrder";
	private static Logger logger;

	@Override
	public int run(String[] args) throws Exception {
		Path partitionFile = new Path(Utils.getUniqueOutputFolder(appName, args[1]) + "/partitions.lst");
		Path outputStage = new Path(Utils.getUniqueOutputFolder(appName, args[1]) + "/staging");
		
		// Configure job to prepare for sampling
		Job sampleJob = Job.getInstance(getConf(), appName);		
		sampleJob.setJarByClass(TotalOrderDriver.class);
		
		sampleJob.setMapperClass(LastRevisionDateMapper.class);
		sampleJob.setNumReduceTasks(0);
		
		sampleJob.setOutputKeyClass(Text.class);
		sampleJob.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(sampleJob, new Path(args[0]));
		sampleJob.setInputFormatClass(WikipediaPageInputFormat.class);
		
		// Set the output format to a sequence file
		sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);
		
		// Submit the job and get completion code.
		int code = sampleJob.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			Job orderJob = Job.getInstance(getConf(), "TotalOrderSortingStage");
			orderJob.setJarByClass(TotalOrderDriver.class);
			// Here, use the identity mapper to output the key/value pairs in
			// the SequenceFile
			orderJob.setMapperClass(OrderMapper.class);
			orderJob.setReducerClass(ValueReducer.class);
			// Set the number of reduce tasks to an appropriate number for the
			// amount of data being sorted
			orderJob.setNumReduceTasks(2);
			// Use Hadoop's TotalOrderPartitioner class
			orderJob.setPartitionerClass(TotalOrderPartitioner.class);
			// Set the partition file
			TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),
					partitionFile);
			orderJob.setOutputKeyClass(Text.class);
			orderJob.setOutputValueClass(Text.class);
			// Set the input to the previous job's output
			orderJob.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPaths(orderJob, outputStage);
			// Set the output path to the command line parameter
			TextOutputFormat.setOutputPath(orderJob,
					new Path(Utils.getUniqueOutputFolder(appName, args[1]) + "2"));
			// Set the separator to an empty string
			orderJob.getConfiguration().set(
					"mapred.textoutputformat.separator", "");
			// Use the InputSampler to go through the output of the previous
			// job, sample it, and create the partition file
			InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler<Text, Text>(.2, 1000));
			// Submit the job
			code = orderJob.waitForCompletion(true) ? 0 : 2;
		}
		// Clean up the partition file and the staging directory
		FileSystem.get(new Configuration()).delete(partitionFile, false);
		FileSystem.get(new Configuration()).delete(
				new Path(Utils.getUniqueOutputFolder(appName, args[1])
						+ "_staging"), true);
		return code;
	}

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

		ToolRunner.run(new TotalOrderDriver(), args);
	}

}
