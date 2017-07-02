package mapreduce.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class job {
	public static enum MyCounter {
		//ö��
		mc
	}
	public static void main(String[] args) {
		//�����ļ����ã��������У�
		Configuration conf = new Configuration();
		// �������з�ʽ�����hdfs����ں�resourcemanager�Ľڵ��ַ��
		conf.set("fs.defaultFS", "hdfs://192.168.229.168:9000");
		conf.set("yarn.resourcemanager.hostname", "192.168.229.168");
		
		//����ֵ
		double d = 0.001;
		int i = 0;
		while (true) {
			i++;
			try {
				// ��¼����Ĵ���
				conf.setInt("runCount", i);
				
				FileSystem fs = FileSystem.get(conf);
				Job job = Job.getInstance(conf);
				
				job.setJarByClass(job.class);
				job.setJobName("pr"+i);
				job.setMapperClass(mapper.class);
				job.setReducerClass(reducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(KeyValueTextInputFormat.class);
				// ������� ·��
				Path inputpath = new Path("/mapreduce/pagerank/input/pagerank.txt");
				
				if (i > 1) {
					inputpath = new Path("/mapreduce/pagerank/pr"+(i-1));
				}
				FileInputFormat.addInputPath(job, inputpath);
				
				Path outputpath = new Path("/mapreduce/pagerank/pr"+i);
				
				if(fs.exists(outputpath)) {
					fs.delete(outputpath,true);
				}
				FileOutputFormat.setOutputPath(job, outputpath);
				
				boolean f = job.waitForCompletion(true);
				
				if (f) {
					System.out.println("Success~~~");
					//��ȡ�������еĲ�ֵ
					long sum = job.getCounters().findCounter(MyCounter.mc).getValue();
					
					System.out.println("SUM: "+sum);
					double avgd = sum /4000.0;
					if (avgd < d) {
						break;
						
					}
				}
				System.out.println(f);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
