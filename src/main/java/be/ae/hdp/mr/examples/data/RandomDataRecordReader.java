package be.ae.hdp.mr.examples.data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.jfairy.Fairy;
import org.jfairy.producer.person.Person;
import org.jfairy.producer.person.PersonProperties;

public class RandomDataRecordReader extends RecordReader<Text, NullWritable>{

	private int numRecordsToCreate = 0;
	private int createdRecords = 0;
	private Text key = new Text();
	private NullWritable value = NullWritable.get();
	private Fairy fairy = Fairy.create();
	private ArrayList<String> randomProducts = new ArrayList<String>();
	
	public static final String NUM_MAP_TASKS = "random.generator.num.map.tasks";
	public static final String NUM_RECORDS_PER_TASKS = "random.generator.num.records.per.map.task";
	public static final String RANDOM_WORD_LIST = "random.generator.random.word.list";
	
	private static final Logger logger = Logger.getLogger(RandomDataRecordReader.class);
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		logger.debug("Initializing");
		numRecordsToCreate = context.getConfiguration().getInt(NUM_RECORDS_PER_TASKS, -1);
		URI[] files = context.getCacheFiles();
		
		BufferedReader reader = new BufferedReader(new FileReader(files[0].toString()));
		
		String line;
		while((line = reader.readLine()) != null){
			randomProducts.add(line);
		}
		logger.debug(String.format("Added %d random product lines", randomProducts.size()));
		reader.close();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(createdRecords < numRecordsToCreate){
			Person person = fairy.person(PersonProperties.minAge(16));
			String record = fairy.dateProducer().randomDateInThePast(3) + "," +
							fairy.network().ipAddress() + "," +
							person.fullName() + "," + 
							person.age() + "," + 
							person.username() + ", " +
							fairy.baseProducer().randomBetween(0.5, 200) + "," +
							fairy.baseProducer().randomElement(randomProducts);
			
			key.set(record);
			createdRecords++;
			
			return true;
		} else
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)createdRecords/(float)numRecordsToCreate;
	}

	@Override
	public void close() throws IOException {
		
	}

}
