package mapreduce.fof;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] strs = StringUtils.split(value.toString(),' ');
		
		for(int i=0 ; i<strs.length ; i++) {
			//一度关系
			String s = nameFormat(strs[0],strs[i]);
			context.write(new Text(s), new IntWritable(0));
			for(int j = i+1;j<strs.length ; j++) {
				//二度关系
				String ss = nameFormat(strs[i], strs[j]);
				context.write(new Text(ss), new IntWritable(1));
				
			}
		}
	}

	private String nameFormat(String s1, String s2) {
		int c = s1.compareTo(s2);
		if(c<0) return s2+"-"+s1;
		return s1+"-"+s2;
	}
	

}
