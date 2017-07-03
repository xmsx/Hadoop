package mapreduce.itemcf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * ItemCF 基于物品的协同过滤算法
 * 
 * @author MFS
 * 
 */
public class startrun {

	public static void main(String[] args) {
		// 默认加载 scr 下的配置文件
		Configuration conf = new Configuration();

		// 本地运行方式（添加hdfs的入口和resourcemanager的节点地址）
		conf.set("fs.defaultFS", "hdfs://192.168.229.168:9000");
		conf.set("yarn.resourcemanager.hostname", "192.168.229.168");

		// 所有mr的输入和输出目录定义在map集合中
		Map<String, String> paths = new HashMap<String, String>();
		paths.put("step1Input", "/mapreduce/itemCF/input/data.csv");
		paths.put("step1Output", "/mapreduce/itemCF/output/step1");
		paths.put("step2Input", paths.get("step1Output"));
		paths.put("step2Output", "/mapreduce/itemCF/output/step2");
		paths.put("step3Input", paths.get("step2Output"));
		paths.put("step3Output", "/mapreduce/itemCF/output/step3");
		paths.put("step4Input1", paths.get("step2Output"));
		paths.put("step4Input2", paths.get("step3Output"));
		paths.put("step4Output", "/mapreduce/itemCF/output/step4");
		paths.put("step5Input", paths.get("step4Output"));
		paths.put("step5Output", "/mapreduce/itemCF/output/step5");
		paths.put("step6Input", paths.get("step5Output"));
		paths.put("step6Output", "/mapreduce/itemCF/output/step6");

//		step1.run(conf, paths);
//		step2.run(conf, paths);
//		step3.run(conf, paths);
//		step4.run(conf, paths);
//		step5.run(conf, paths);
		step6.run(conf, paths);
	}

	public static Map<String, Integer> R = new HashMap<String, Integer>();
	static {
		R.put("click", 1);
		R.put("collect", 2);
		R.put("cart", 3);
		R.put("alipay", 4);
	}
}
