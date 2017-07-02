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
				//����ǰ
				sourcenode = node;
			} else {
				//
				sum += node.getPageRank();
			}
		}
		
		//�����µ�PRֵ ��4.0 Ϊҳ��������0.85ΪGoogle������ϵ��(����)
		double newPR = (0.15/4.0) + (0.85 * sum);
		System.out.println("********new pagerank value is "+ newPR +"********");
		
		//���µ�PRֵ�ͼ���ǰ��PR�Ƚ�
		double d = newPR - sourcenode.getPageRank();
		int j = (int)(d*1000.0);
		j = Math.abs(j);
		System.out.println(j+"__________");
		//�ۼ�
		context.getCounter(MyCounter.mc).increment(j);
		
		sourcenode.setPageRank(newPR);
		context.write(key, new Text(sourcenode.toString()));
		
	}
	
}
