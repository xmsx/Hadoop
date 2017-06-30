/**
 * 月温度top 2
 * 题目：
 * 	给出日期，求每月温度前2个
 * 方法：
 * 	设计一个  weather 的  bean 实现WritableComparable<Weather>接口，重写比较方法。
 * 	重写partition类，重定义分组方式。（决定数据由哪个reducer来处理）
 * 	继承WritableComparator类，重定义mapper端的merge的合并方法，按照年份降序，温度升序的方法。
 * 		（原本默认merge方法是按照字典序排序，新方法使年份可排序。）
 * 	继承WritableComparator类，重定义reducer端的group的分组方法，由于此时温度为value的值，所以值对年份排序即可。
 * 		（原本默认group方法是按照字典序排序，新方法使年份可排序。）
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
		job.setMapOutputKeyClass(Weather.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 重写shuffle的类方法
		job.setPartitionerClass(partition.class);
		job.setSortComparatorClass(sort.class);
		job.setGroupingComparatorClass(group.class);
		//指定reducer的个数
		job.setNumReduceTasks(3);
		
		System.out.println("-------2");
		// 指定reducer类
		job.setReducerClass(reducer.class);
		// 获取作业对象文件地址
		FileInputFormat.addInputPath(job, new Path("/mapreduce/temperature/input/temperature"));
		
		System.out.println("-------3");
		// 输出作业结果到HDFS
		Path outputpath = new Path("/mapreduce/temperature/output/");
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
