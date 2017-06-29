package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// reducer 接受的为  mapper输出shuffle后的内容，泛型<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,Context context) 
			throws IOException, InterruptedException {
		// shuffle 后相同的  key 将分给同一个reducer，求和即可
		int sum = 0;
		// 迭代器访问  shuffle 后的内容，用迭代器遍历个数即可（ i 获取的是  value ，都为 1） 
		for (IntWritable i : iterable) {
			sum += i.get();
		}
		
		context.write(text, new IntWritable(sum));
	}
	
	

}
