package br.ufc.great.es.tsd.mapreduce.words;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordAverage {

	public static class TokenizerAverageMapper extends Mapper<Object, Text, Text, IntWritable>{
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	      
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      
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

	public static class AverageWordLength extends Reducer<Text,IntWritable,Text,IntWritable> {   
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0; 
			int count = 0; 
			int Average = 0;
			
			for(IntWritable val : values) {
				sum = sum +  val.get(); 
				count = count+1;
			}
			
			Average = sum/count;
			result.set(Average);
			context.write(key,result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordaverage <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "word average");
		job.setJarByClass(WordAverage.class);
		job.setMapperClass(TokenizerAverageMapper.class);
		job.setCombinerClass(AverageWordLength.class);
		job.setReducerClass(AverageWordLength.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}