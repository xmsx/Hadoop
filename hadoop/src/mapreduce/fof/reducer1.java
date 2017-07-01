package mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer1 extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> it,Context context) 
			throws IOException, InterruptedException {
		int sum = 0;
		boolean f = true;
		for(IntWritable i : it) {
			if( i.get() == 0 ){
				f = false;
				break;
			}
			sum += i.get();
		}
		if(f){
			String s = text.toString()+"-"+sum;
			context.write(new Text(s), NullWritable.get());
		}
	}
	
}
