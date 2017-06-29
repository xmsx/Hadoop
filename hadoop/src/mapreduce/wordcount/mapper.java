package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

// 泛型为<偏移量（行号）,全部文本输入,文本输出,数量输出>
public class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// 获取整个文本
		String str = value.toString();
		// 将整个文本用 " "分割
		String[] strs = StringUtils.split(str,' ');
		// 存入 context，然后交给 shuffle 处理
		for (String s : strs){
			context.write(new Text(s), new IntWritable(1));
		}
	}
	
	

}
