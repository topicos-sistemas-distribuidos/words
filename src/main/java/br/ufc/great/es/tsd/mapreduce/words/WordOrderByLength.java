package br.ufc.great.es.tsd.mapreduce.words;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import br.ufc.great.es.tsd.mapreduce.words.orderby.MyWord;
import br.ufc.great.es.tsd.mapreduce.words.orderby.MyWordGroupingComparator;
import br.ufc.great.es.tsd.mapreduce.words.orderby.WordMapper;
import br.ufc.great.es.tsd.mapreduce.words.orderby.WordPartitioner;
import br.ufc.great.es.tsd.mapreduce.words.orderby.WordSizeReducerTask;
import br.ufc.great.es.tsd.mapreduce.words.orderby.WordSortingComparator;

public class WordOrderByLength {
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 2) {
			System.err.println("Usage: secondarysort <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "word sort");
		job.setJarByClass(WordOrderByLength.class);
		job.setMapperClass(WordMapper.class);
	    job.setCombinerClass(WordSizeReducerTask.class);
	    job.setReducerClass(WordSizeReducerTask.class);

		job.setPartitionerClass(WordPartitioner.class);
		job.setSortComparatorClass(WordSortingComparator.class);
		job.setGroupingComparatorClass(MyWordGroupingComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(2);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}