package be.ae.hdp.mr.examples.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomProductDataInputFormat extends InputFormat<Text, NullWritable>{

	public static final String NUM_MAP_TASKS = "random.generator.num.map.tasks";
	public static final String NUM_RECORDS_PER_TASKS = "random.generator.num.records.per.map.task";
	public static final String RANDOM_WORD_LIST = "random.generator.random.word.list";
	
	@Override
	public List getSplits(JobContext context) throws IOException,
			InterruptedException {
				int numSplits = context.getConfiguration().getInt(NUM_MAP_TASKS, -1);
		
		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
		for(int i = 0; i < numSplits; i++){
			splits.add(new FakeInputSplit());
		}
		return splits;
	}

	@Override
	public RecordReader createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		RandomDataRecordReader rr = new RandomDataRecordReader();
		rr.initialize(split, context);
		return rr;
	}
	
	public static void setNumMapTasks(Job job, int i){
		job.getConfiguration().setInt(NUM_MAP_TASKS, i);
	}
	
	public static void setNumRecordsPerMapTask(Job job, int i){
		job.getConfiguration().setInt(NUM_RECORDS_PER_TASKS, i);
	}
	
	public static void setRandomWordList(Job job, Path file){
		job.addCacheFile(file.toUri());
	}
	

}
