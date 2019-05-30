package br.ufc.great.es.tsd.mapreduce.words.orderby;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author armandosoaressousa
 *
 */
public class WordMapper extends Mapper<Object, Text, MyWord, IntWritable>{

	MyWord w = new MyWord();
	public void map(Text fWord, IntWritable lSize, Context context) throws IOException, InterruptedException{
		
		w.setFword(fWord);
		w.setLsize(lSize);
		context.write(w, lSize);		
	}

}
