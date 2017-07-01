package mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class job2 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		// Ĭ�ϼ��� scr �µ������ļ�
		Configuration conf = new Configuration();

		// �������з�ʽ�����hdfs����ں�resourcemanager�Ľڵ��ַ��
		conf.set("fs.defaultFS", "hdfs://192.168.229.168:9000");
		conf.set("yarn.resourcemanager.hostname", "192.168.229.168");

		// �����ϴ�������������䣨��Ҫ�������ļ���
		// conf.set("mapred.jar", "E:\\hadoop\\mr\\WordCount.jar");

		// ���� MR �ĵ�ַ
		Job job = Job.getInstance(conf);
		// ָ����������
		job.setJarByClass(job2.class);

		System.out.println("-------1");
		// ָ��mapper��
		job.setMapperClass(mapper2.class);
		// ָ��map����
		job.setMapOutputKeyClass(Rela.class);
		job.setMapOutputValueClass(IntWritable.class);
		//
		job.setSortComparatorClass(sort.class);
		job.setGroupingComparatorClass(group.class);

		System.out.println("-------2");
		// ָ��reducer��
		job.setReducerClass(reducer2.class);
		// ��ȡ��ҵ�����ļ���ַ
		FileInputFormat.addInputPath(job, new Path("/mapreduce/FOF/output"));

		System.out.println("-------3");
		// �����ҵ�����HDFS
		Path outputpath = new Path("/mapreduce/FOF/outputfinal/");
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
