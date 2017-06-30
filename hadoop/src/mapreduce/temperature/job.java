/**
 * ���¶�top 2
 * ��Ŀ��
 * 	�������ڣ���ÿ���¶�ǰ2��
 * ������
 * 	���һ��  weather ��  bean ʵ��WritableComparable<Weather>�ӿڣ���д�ȽϷ�����
 * 	��дpartition�࣬�ض�����鷽ʽ���������������ĸ�reducer������
 * 	�̳�WritableComparator�࣬�ض���mapper�˵�merge�ĺϲ�������������ݽ����¶�����ķ�����
 * 		��ԭ��Ĭ��merge�����ǰ����ֵ��������·���ʹ��ݿ����򡣣�
 * 	�̳�WritableComparator�࣬�ض���reducer�˵�group�ķ��鷽�������ڴ�ʱ�¶�Ϊvalue��ֵ������ֵ��������򼴿ɡ�
 * 		��ԭ��Ĭ��group�����ǰ����ֵ��������·���ʹ��ݿ����򡣣�
 * 
 */
package mapreduce.temperature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
		job.setMapOutputKeyClass(Weather.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// ��дshuffle���෽��
		job.setPartitionerClass(partition.class);
		job.setSortComparatorClass(sort.class);
		job.setGroupingComparatorClass(group.class);
		//ָ��reducer�ĸ���
		job.setNumReduceTasks(3);
		
		System.out.println("-------2");
		// ָ��reducer��
		job.setReducerClass(reducer.class);
		// ��ȡ��ҵ�����ļ���ַ
		FileInputFormat.addInputPath(job, new Path("/mapreduce/temperature/input/temperature"));
		
		System.out.println("-------3");
		// �����ҵ�����HDFS
		Path outputpath = new Path("/mapreduce/temperature/output/");
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
