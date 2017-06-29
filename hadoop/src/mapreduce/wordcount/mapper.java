package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

// ����Ϊ<ƫ�������кţ�,ȫ���ı�����,�ı����,�������>
public class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// ��ȡ�����ı�
		String str = value.toString();
		// �������ı��� " "�ָ�
		String[] strs = StringUtils.split(str,' ');
		// ���� context��Ȼ�󽻸� shuffle ����
		for (String s : strs){
			context.write(new Text(s), new IntWritable(1));
		}
	}
	
	

}
