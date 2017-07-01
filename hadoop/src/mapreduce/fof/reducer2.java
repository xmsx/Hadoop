package mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer2 extends Reducer<Rela, IntWritable, Text, NullWritable>{

	@Override
	protected void reduce(Rela rela, Iterable<IntWritable> it,
			Context context) throws IOException, InterruptedException {
		for(IntWritable i : it) {
			String msg = rela.getF1()+"-"+rela.getF2()+":"+i.get();
			context.write(new Text(msg), NullWritable.get());
		}
	}

}
