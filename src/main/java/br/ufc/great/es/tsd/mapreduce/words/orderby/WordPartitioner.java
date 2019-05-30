package br.ufc.great.es.tsd.mapreduce.words.orderby;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<MyWord, IntWritable>{

	@Override
	public int getPartition(MyWord key, IntWritable value, int nuOfReducers) {
		// TODO Auto-generated method stub
		return (key.getLsize().get() % nuOfReducers);
	}

}
