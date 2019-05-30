package br.ufc.great.es.tsd.mapreduce.words.orderby;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * map(k1, v1) -> reduce(k2, v2)
 * Dado o map(palavra, tamanho) -> faz o reduce(palavra, tamanho)
 * @author armandosoaressousa
 *
 */
public class WordSizeReducerTask  extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int size = 0;
      for (IntWritable val : values) {
        size = val.get();
      }
      result.set(size);
      context.write(key, result);
    }
  }