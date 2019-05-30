package br.ufc.great.es.tsd.mapreduce.words.orderby;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MyWord implements WritableComparable<MyWord>{

	private IntWritable lsize; 
	private Text fword;
	
	public MyWord() {
		fword= new Text();
		lsize= new IntWritable();
	}
	
	public MyWord(String word, int size) {
		this(new Text(word), new IntWritable(size));
	}
	
	public MyWord(Text word, IntWritable size) {
		this.fword=word;
		this.lsize=size;
	}
	
	public IntWritable getLsize() {
		return lsize;
	}
	public void setLsize(IntWritable lsize) {
		this.lsize = lsize;
	}
	
	public Text getFname() {
		return fword;
	}
	
	public void setFword(Text fword) {
		this.fword = fword;
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		lsize.readFields(in);
		fword.readFields(in);	
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		lsize.write(out);
		fword.write(out);
	}

	public int compareTo(MyWord o) {
		// TODO Auto-generated method stub
		int cmp = lsize.compareTo(o.lsize);
		if (cmp==0){
			return fword.compareTo(o.fword);
		}
		return cmp;
	}

}