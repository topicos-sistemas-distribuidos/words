package br.ufc.great.es.tsd.mapreduce.words.orderby;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<MyWord, IntWritable, MyWord, IntWritable>{

	public void reduce(MyWord key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
		for(IntWritable lsize:value){
			context.write(key, lsize);
		}

	}

}
