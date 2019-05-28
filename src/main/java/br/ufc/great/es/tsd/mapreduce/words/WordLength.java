package br.ufc.great.es.tsd.mapreduce.words;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordLength {

	/**
	 * Classe que faz a leitura do arquivo texto e agrupa map(key, value)
	 * key = tamanho da palavra
	 * value = palavra
	 * @author armandosoaressousa
	 *
	 */
	public static class TokenizerMapper extends Mapper<Object, IntWritable, Text, IntWritable>{
		private final static IntWritable oneWordLength = new IntWritable(1);
		private Text wordKey = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {
				String myKey = itr.nextToken();
				myKey = myKey.replaceAll ( "[^A-Za-z0-9]",""); 
				int myValue = myKey.length();
				wordKey.set(myKey); // key = palavra
				oneWordLength.set(myValue); // value = tamanho da palavra
				context.write(wordKey, oneWordLength); // map(key, value) = map(palavra, tamanho da palavra)
			}

		}

	}

	public static class SizeWordReducerTask extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable resultKey = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int size = 0;
			for (IntWritable val : values) {
				size = val.get();
			}
			resultKey.set(size);
			context.write(key, resultKey);
		}
	}

	/**
	 * Classe que faz o reduce agrupando os fragmentos (key, value) em um único resultado
	 * @author armandosoaressousa
	 *
	 */
	public static class ReduceTaskSwap extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text keyWord, Iterable<IntWritable> list, Context context) throws java.io.IOException, InterruptedException {
			for (IntWritable valueOfWordLength : list) {
				context.write(keyWord, valueOfWordLength);
			}
		}
	}

	/**
	 * Classe que faz a ordenacao descendente dos resultados (key, value) ordenados por key
	 * @author armandosoaressousa
	 *
	 */
	public static class DescendingKeyComparator extends WritableComparator {
		protected DescendingKeyComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable key1 = (IntWritable) w1;
			IntWritable key2 = (IntWritable) w2;          
			return -1 * key1.compareTo(key2);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "word length");
		job.setJarByClass(WordLength.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(SizeWordReducerTask.class);
		job.setReducerClass(SizeWordReducerTask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}