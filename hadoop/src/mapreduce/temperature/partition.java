package mapreduce.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class partition extends Partitioner<Weather, IntWritable> {

	@Override
	public int getPartition(Weather key, IntWritable value, int numReduceTasks) {

		return (key.getYear() - 1949) % numReduceTasks;
//		return super.getPartition(key, value, numReduceTasks);
	}
	

}
