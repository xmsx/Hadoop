package mapreduce.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class sort extends WritableComparator {
	public sort(){
		super(Rela.class , true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Rela rela0 = (Rela)a;
		Rela rela1 = (Rela)b;
		
		int c = rela0.compareTo(rela1);
		if(c == 0){
			return -Integer.compare(rela0.getHot(), rela1.getHot());
		}
		return c;
	}
	
}
