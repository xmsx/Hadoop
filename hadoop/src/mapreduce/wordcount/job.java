/**
 * mapreduce 本地与运行方式（在jobhistory http://192.168.229.168:8088 中没有记录）
 * 注意：
 * 	1.需要指定hdfs的入口和resourcemanager的节点地址
 * 	2.不要在scr文件夹下存放服务器集群的配置文件
 * 
 * （上传作业到服务器的方式暂时没有解决，可采用将作业打成jar包，
 * 	发送到服务器用 hadoop jar wordcount.jar mapreduce.WordCount 命令执行（以本作业为例））
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
		
		// 默认加载 scr 下的配置文件
		Configuration conf = new Configuration();
		
		// 本地运行方式（添加hdfs的入口和resourcemanager的节点地址）
		conf.set("fs.defaultFS", "hdfs://192.168.229.168:9000");
		conf.set("yarn.resourcemanager.hostname", "192.168.229.168");
		
		// 本地上传至服务器的语句（需要加配置文件）
		//conf.set("mapred.jar", "E:\\hadoop\\mr\\WordCount.jar");
		
		// 管理 MR 的地址
		Job job = Job.getInstance(conf);
		// 指定程序的入口
		job.setJarByClass(job.class);
		
		System.out.println("-------1");
		// 指定mapper类
		job.setMapperClass(mapper.class);
		// 指定map类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		System.out.println("-------2");
		// 指定reducer类
		job.setReducerClass(reducer.class);
		// 获取作业对象文件地址
		FileInputFormat.addInputPath(job, new Path("/wordcount/input/WordInput"));
		
		System.out.println("-------3");
		// 输出作业结果到HDFS
		Path outputpath = new Path("/wordcount/output/");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputpath)) {
			fs.delete(outputpath, true);
		}
		FileOutputFormat.setOutputPath(job, outputpath);
		
		System.out.println("-------4");
		// 显示控制台结果
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag);
	}

}
