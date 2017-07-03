package mapreduce.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * ���û����飬����������Ʒ���ֵ�����б��õ��û�����Ʒ��ϲ���ȵ÷־���
	u13	i160:1,
	u14	i25:1,i223:1,
	u16	i252:1,
	u21	i266:1,
	u24	i64:1,i218:1,i185:1,
	u26	i276:1,i201:1,i348:1,i321:1,i136:1,
 * @author MFS
 */
public class step2 {

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			
			job.setJobName("step2");
			job.setJarByClass(startrun.class);
			job.setMapperClass(step2_Mapper.class);
			job.setReducerClass(step2_Reducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(paths.get("step2Input")));
			Path outpath = new Path(paths.get("step2Output"));
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

	static class step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

		// ���ʹ�ã��Ñ�+��Ʒ��ͬʱ��Ϊ���key������
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String item = tokens[0];
			String user = tokens[1];
			String action = tokens[2];
			Text k = new Text(user);
			Integer rv = startrun.R.get(action);
			// if(rv!=null){
			Text v = new Text(item + ":" + rv.intValue());
			context.write(k, v);
		}
	}

	static class step2_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> i, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> r = new HashMap<String, Integer>();

			
			for (Text value : i) {
				String[] vs = value.toString().split(":");
				String item = vs[0];
				Integer action = Integer.parseInt(vs[1]);
				action = ((Integer) (r.get(item) == null ? 0 : r.get(item))).intValue() + action;
				r.put(item, action);
			}
			StringBuffer sb = new StringBuffer();
			for (Entry<String, Integer> entry : r.entrySet()) {
				sb.append(entry.getKey() + ":" + entry.getValue().intValue() + ",");
			}

			context.write(key, new Text(sb.toString()));
		}
	}
}