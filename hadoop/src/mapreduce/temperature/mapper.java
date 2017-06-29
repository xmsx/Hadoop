package mapreduce.temperature;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class mapper extends Mapper<LongWritable, Text, Weather, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strs = StringUtils.split(value.toString(), '\t');
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(sdf.parse(strs[0]));
			
			Weather w = new Weather();
			w.setYear(cal.get(Calendar.YEAR));
			w.setMonth(cal.get(Calendar.MONTH)+1);
			
			int temp = Integer.parseInt(strs[1].substring(0, strs[1].lastIndexOf('c')));
			w.setTemp(temp);
			
			context.write(w, new IntWritable(temp));
		} catch (ParseException e){
			e.printStackTrace();
		}
	}
	

}
