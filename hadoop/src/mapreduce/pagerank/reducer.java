package mapreduce.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.pagerank.job.MyCounter;

public class reducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> iterator, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		Node sourcenode = null;
		for (Text i : iterator) {
			Node node = Node.fromMR(i.toString());
			//
			if(node.containsAdjacentNodes()) {
				//计算前
				sourcenode = node;
			} else {
				//
				sum += node.getPageRank();
			}
		}
		
		//计算新的PR值 ，4.0 为页面总数，0.85为Google的阻尼系数(配重)
		double newPR = (0.15/4.0) + (0.85 * sum);
		System.out.println("********new pagerank value is "+ newPR +"********");
		
		//把新的PR值和计算前的PR比较
		double d = newPR - sourcenode.getPageRank();
		int j = (int)(d*1000.0);
		j = Math.abs(j);
		System.out.println(j+"__________");
		//累加
		context.getCounter(MyCounter.mc).increment(j);
		
		sourcenode.setPageRank(newPR);
		context.write(key, new Text(sourcenode.toString()));
		
	}
	
}
