package mapreduce.fof;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper2 extends Mapper<LongWritable, Text, Rela, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strs = StringUtils.split(value.toString(),'-');
		Rela rela0 = new Rela();
		rela0.setF1(strs[0]);
		rela0.setF2(strs[1]);
		rela0.setHot(Integer.parseInt(strs[2]));
		context.write(rela0, new IntWritable(rela0.getHot()));
		
		Rela rela1 = new Rela();
		rela1.setF1(strs[1]);
		rela1.setF2(strs[0]);
		rela1.setHot(Integer.parseInt(strs[2]));
		context.write(rela1, new IntWritable(rela1.getHot()));
		
	}
	
}
