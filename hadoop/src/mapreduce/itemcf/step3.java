package mapreduce.itemcf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ����Ʒ����б���м�����������Ʒ��ͬ�־���
	i100:i100	3
	i100:i105	1
	i100:i106	1
	i100:i109	1
	i100:i114	1
	i100:i124	1
 * @author MFS
 */
public class step3 {
	private final static Text K = new Text();
	private final static IntWritable V = new IntWritable(1);

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("step3");
			job.setJarByClass(startrun.class);
			job.setMapperClass(step3_Mapper.class);
			job.setReducerClass(step3_Reducer.class);
			job.setCombinerClass(step3_Reducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			FileInputFormat
					.addInputPath(job, new Path(paths.get("step3Input")));
			Path outpath = new Path(paths.get("step3Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	// �ڶ���MRִ�еĽ��--��Ϊ����MR������  ����: u2837	 i541:1,i331:1,i314:1,i125:1,
	static class step3_Mapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String[] items = tokens[1].split(",");
			for (int i = 0; i < items.length; i++) {
				String itemA = items[i].split(":")[0];
				for (int j = 0; j < items.length; j++) {
					String itemB = items[j].split(":")[0];
					K.set(itemA + ":" + itemB);
					context.write(K, V);
				}
			}

		}
	}
	

	static class step3_Reducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> i, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : i) {
				sum = sum + v.get();
			}
			V.set(sum);
			context.write(key, V);
			//  ִ�н��
//			i100:i181	1
//			i100:i184	2
		}
	}
}

