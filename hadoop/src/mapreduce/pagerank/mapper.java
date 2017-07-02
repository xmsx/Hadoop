package mapreduce.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<Text, Text, Text, Text>{

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		int runCount = context.getConfiguration().getInt("runCount", 1);
		
		String page = key.toString();
		Node node = null;
		// 第一次计算初始化PR值为  1.0
		if (runCount == 1) {
			node = Node.fromMR("1.0"+"\t"+value.toString());
			
		} else {
			node = Node.fromMR(value.toString());

		}
		// A:1.0 B D
		//将计算的数值输出 ，reduce计算做差值
		context.write(new Text(page), new Text(node.toString()));
		
		if (node.containsAdjacentNodes()) {
			double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
			for (int i=0 ; i<node.getAdjacentNodeNames().length ; i++) {
				String outpage = node.getAdjacentNodeNames()[i];
				//
				//
				context.write(new Text(outpage), new Text(outValue+""));
			}
		}
		
	}
	

}
