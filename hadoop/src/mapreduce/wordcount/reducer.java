package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// reducer ���ܵ�Ϊ  mapper���shuffle������ݣ�����<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,Context context) 
			throws IOException, InterruptedException {
		// shuffle ����ͬ��  key ���ָ�ͬһ��reducer����ͼ���
		int sum = 0;
		// ����������  shuffle ������ݣ��õ����������������ɣ� i ��ȡ����  value ����Ϊ 1�� 
		for (IntWritable i : iterable) {
			sum += i.get();
		}
		
		context.write(text, new IntWritable(sum));
	}
	
	

}
