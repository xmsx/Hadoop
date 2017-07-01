package mapreduce.fof;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Rela implements WritableComparable<Rela> {
	private String f1;
	private String f2;
	private int hot;
	
	

	public String getF1() {
		return f1;
	}

	public void setF1(String f1) {
		this.f1 = f1;
	}

	public String getF2() {
		return f2;
	}

	public void setF2(String f2) {
		this.f2 = f2;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.f1 = in.readUTF();
		this.f2 = in.readUTF();
		this.hot = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(f1);
		out.writeUTF(f2);
		out.writeInt(hot);
		
	}

	@Override
	public int compareTo(Rela o) {
		int c = this.f1.compareTo(o.getF1());
		if(c == 0){
			return Integer.compare(hot, o.getHot());
		}
		return c;
	}
	

}
