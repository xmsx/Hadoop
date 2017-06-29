/**
 * mapreduce ���������з�ʽ����jobhistory http://192.168.229.168:8088 ��û�м�¼��
 * ע�⣺
 * 	1.��Ҫָ��hdfs����ں�resourcemanager�Ľڵ��ַ
 * 	2.��Ҫ��scr�ļ����´�ŷ�������Ⱥ�������ļ�
 * 
 * ���ϴ���ҵ���������ķ�ʽ��ʱû�н�����ɲ��ý���ҵ���jar����
 * 	���͵��������� hadoop jar wordcount.jar mapreduce.WordCount ����ִ�У��Ա���ҵΪ������
 * 
 * 
 */
package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class job {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		// Ĭ�ϼ��� scr �µ������ļ�
		Configuration conf = new Configuration();
		
		// �������з�ʽ�����hdfs����ں�resourcemanager�Ľڵ��ַ��
		conf.set("fs.defaultFS", "hdfs://192.168.229.168:9000");
		conf.set("yarn.resourcemanager.hostname", "192.168.229.168");
		
		// �����ϴ�������������䣨��Ҫ�������ļ���
		//conf.set("mapred.jar", "E:\\hadoop\\mr\\WordCount.jar");
		
		// ���� MR �ĵ�ַ
		Job job = Job.getInstance(conf);
		// ָ����������
		job.setJarByClass(job.class);
		
		System.out.println("-------1");
		// ָ��mapper��
		job.setMapperClass(mapper.class);
		// ָ��map����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		System.out.println("-------2");
		// ָ��reducer��
		job.setReducerClass(reducer.class);
		// ��ȡ��ҵ�����ļ���ַ
		FileInputFormat.addInputPath(job, new Path("/wordcount/input/WordInput"));
		
		System.out.println("-------3");
		// �����ҵ�����HDFS
		Path outputpath = new Path("/wordcount/output/");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputpath)) {
			fs.delete(outputpath, true);
		}
		FileOutputFormat.setOutputPath(job, outputpath);
		
		System.out.println("-------4");
		// ��ʾ����̨���
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag);
	}

}
