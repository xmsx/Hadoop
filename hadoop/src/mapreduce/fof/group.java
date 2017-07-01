package mapreduce.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class group extends WritableComparator {
	public group(){
		super(Rela.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Rela rela0 = (Rela)a;
		Rela rela1 = (Rela)b;
		return rela0.compareTo(rela1);
	}

}
