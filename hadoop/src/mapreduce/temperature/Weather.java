package mapreduce.temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class Weather implements WritableComparable<Weather> {
	
	private int year;
	private int month;
	// ÎÂ¶È
	private int temp;

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getTemp() {
		return temp;
	}

	public void setTemp(int temp) {
		this.temp = temp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.temp = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(temp);
		
	}

	@Override
	public int compareTo(Weather w) {
		int c1 = Integer.compare(this.year, w.getYear());
		if(c1 == 0){
			int c2 = Integer.compare(this.month, w.getMonth());
			if(c2 == 0){
				return Integer.compare(this.temp, w.getTemp());
			}
			return c2;
		}
		return c1;
	}

}
