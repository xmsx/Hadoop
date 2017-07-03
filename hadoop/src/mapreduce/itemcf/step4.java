package mapreduce.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * ��ͬ�־���͵÷־������
 * 
 * @author MFS
 */
public class step4 {

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("step4");
			job.setJarByClass(startrun.class);
			job.setMapperClass(step4_Mapper.class);
			job.setReducerClass(step4_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job,
					new Path[] { new Path(paths.get("step4Input1")),
							new Path(paths.get("step4Input2")) });
			Path outpath = new Path(paths.get("step4Output"));
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

	static class step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;// Aͬ�־��� or B�÷־���

		// ÿ��maptask����ʼ��ʱ����һ��
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();// �ж϶������ݼ�

			System.out.println(flag + "**********************");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());

			if (flag.equals("step3")) {// ͬ�־���
				// ����:  i100:i181	1
				//       i100:i184	2
				String[] v1 = tokens[0].split(":");
				String itemID1 = v1[0];
				String itemID2 = v1[1];
				String num = tokens[1];

				Text k = new Text(itemID1);// ��ǰһ����ƷΪkey ����i100
				Text v = new Text("A:" + itemID2 + "," + num);// A:i109,1

				context.write(k, v);

			} else if (flag.equals("step2")) {// �û�����Ʒϲ���÷־���
				// ����:  u24  i64:1,i218:1,i185:1,
				String userID = tokens[0];
				for (int i = 1; i < tokens.length; i++) {
					String[] vector = tokens[i].split(":");
					String itemID = vector[0];// ��Ʒid
					String pref = vector[1];// ϲ������

					Text k = new Text(itemID); // ����ƷΪkey ���磺i100
					Text v = new Text("B:" + userID + "," + pref); // B:u401,2

					context.write(k, v);
				}
			}
		}
	}

	static class step4_Reducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Aͬ�־��� or B�÷־���
			// ĳһ����Ʒ�������������������Ʒ��ͬ�ִ���������mapA������
			Map<String, Integer> mapA = new HashMap<String, Integer>();
			//�͸���Ʒ��key�е�itemID��ͬ�ֵ�������Ʒ��ͬ�ּ���//
			//������ƷIDΪmap��key��ͬ������Ϊֵ
			
			Map<String, Integer> mapB = new HashMap<String, Integer>();
			//����Ʒ��key�е�itemID���������û����Ƽ�Ȩ�ط���

			for (Text line : values) {
				String val = line.toString();
				if (val.startsWith("A:")) {// ��ʾ��Ʒͬ������
					String[] kv = Pattern.compile("[\t,]").split(
							val.substring(2));
					try {
						mapA.put(kv[0], Integer.parseInt(kv[1]));
					} catch (Exception e) {
						e.printStackTrace();
					}

				} else if (val.startsWith("B:")) {
					String[] kv = Pattern.compile("[\t,]").split(
							val.substring(2));
					try {
						mapB.put(kv[0], Integer.parseInt(kv[1]));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			
			double result = 0;
			Iterator<String> iter = mapA.keySet().iterator();
			while (iter.hasNext()) {
				String mapk = iter.next();// itemID

				int num = mapA.get(mapk).intValue();
				Iterator<String> iterb = mapB.keySet().iterator();
				while (iterb.hasNext()) {
					String mapkb = iterb.next();// userID
					int pref = mapB.get(mapkb).intValue();
					result = num * pref;// ����˷���˼���

					Text k = new Text(mapkb);
					Text v = new Text(mapk + "," + result);
					context.write(k, v);
				}
			}
			
			//  �������:  u2723	i9,8.0
		}
	}
}

