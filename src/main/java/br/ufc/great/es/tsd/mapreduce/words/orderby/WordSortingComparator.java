package br.ufc.great.es.tsd.mapreduce.words.orderby;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordSortingComparator extends WritableComparator{

	protected WordSortingComparator() {
		super(MyWord.class,true);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(WritableComparable o1,WritableComparable o2){
		MyWord p1 = (MyWord) o1;
		MyWord p2 = (MyWord) o2;
		int cmp = p1.getLsize().compareTo(p2.getLsize());
		if(cmp == 0){
			cmp = p1.getFname().compareTo(p2.getFname());
		}
		return cmp;
	}

}
