package br.ufc.great.es.tsd.mapreduce.words.orderby;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Faz a leitura de um arquivo texto coloca em um Map(key, value) 
 * key = palavra, value=tamanho da palavra. 
 * Por exemplo: map(armando, 7)
 * @author armandosoaressousa
 *
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      //Faz a leitura arquivo texto e coloca em cada token uma palavra encontrada
      while (itr.hasMoreTokens()) {
  		String myKey = itr.nextToken();
  		myKey = myKey.replaceAll ( "[^A-Za-z0-9]",""); 
  		int myValue = myKey.length();
  		one.set(myValue);
        word.set(myKey);
        context.write(word, one);
      }
      
    }
    
  }