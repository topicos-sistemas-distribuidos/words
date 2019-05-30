package br.ufc.great.es.tsd.mapreduce.words;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordLengthSimple {

	public static class TokenizerMapper extends Mapper<Object, Text, NullWritable, Text>{
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private TreeMap<Integer, Text> topN = new TreeMap<Integer, Text>();
	    
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      
	      while (itr.hasMoreTokens()) {
	  		String myKey = itr.nextToken();
	  		myKey = myKey.replaceAll ( "[^A-Za-z0-9]",""); 
	  		int myValue = myKey.length();
	  		
	  		if (myValue < 2) {
	  			return;
	  		}
	  		
	  		one.set(myValue);
	        word.set(myKey);

	  		topN.put(myValue, word);
	  		
	  		if (topN.size() > 10) {
	  			topN.remove(topN.firstKey());
	  		}
	        
	      }
	      
	    	for (Text t : topN.values()) {
	    		context.write(NullWritable.get(), t);
	    	}
	      
	    }
	    
	    /*
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException{
	    	for (Text t : topN.values()) {
	    		context.write(NullWritable.get(), t);
	    	}
	    }
	    */
	    
	  }
	  
	  public static class WordSizeReducerTask extends Reducer<NullWritable ,Text, NullWritable, Text> {
	    private TreeMap<Integer, Text> topN = new TreeMap<Integer, Text>();

	    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	      for (Text val : values) {
	        String[] words = val.toString().split("\t");	        
	        topN.put(Integer.parseInt(words[1]), new Text(val));
	        
	        if (topN.size() > 10) {
	        	topN.remove(topN.firstKey());
	        }
	      }
	      
	      for (Text word : topN.descendingMap().values()) {
	    	  context.write(NullWritable.get(), word);
	      }
	      
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
			job.setJarByClass(WordLengthSimple.class);
			job.setMapperClass(TokenizerMapper.class);
		    job.setCombinerClass(WordSizeReducerTask.class);
		    job.setReducerClass(WordSizeReducerTask.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			for (int i = 0; i < otherArgs.length - 1; ++i) {
				FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
			}

			FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
}