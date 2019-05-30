package br.ufc.great.es.tsd.mapreduce.words.orderby;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author armandosoaressousa
 *
 */
public class WordMapper extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	MyWord w = new MyWord();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer itr = new StringTokenizer(value.toString());

		//Faz a leitura arquivo texto e coloca em cada token uma palavra encontrada
		while (itr.hasMoreTokens()) {
			String myKey = itr.nextToken();
			myKey = myKey.replaceAll ( "[^A-Za-z0-9]",""); 
			int myValue = myKey.length();
			one.set(myValue);
			word.set(myKey);

			w.setFword(word);
			w.setLsize(one);
			context.write(w.getFname(), w.getLsize());		
		}
	}

}
