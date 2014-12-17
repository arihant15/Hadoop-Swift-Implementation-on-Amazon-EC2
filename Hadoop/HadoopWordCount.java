package hadoop.myhadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HadoopWordCount
{ 
	
	// Mapper<Input Key Type, Input Value Type, Output Key Type, Output Value Type>
	public static class HadoopMap extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		
		private String word;

		public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			String line = value.toString();
			line.trim();

			Pattern p=Pattern.compile("\'?([a-zA-z0-9'-]+)\'?");
			Matcher matcher = p.matcher(line);

			while(matcher.find())
			{
				word = new String(matcher.group());
				word = word.replaceAll("[^A-Za-z0-9]" , "");
				context.write(new Text(word), one);
			}
		}
	}
	
	// Reducer<Input Key Type, Input Value Type, Output Key Type, Output Value Type>
	public static class HadoopReduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception
	{		
		// Create a new Job
		Configuration conf = new Configuration();	    
		Job job = new Job(conf, "Hadoop Word Count"); 
		
		//Setting configuration object with the Data Type of output Key and Value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//Providing the mapper and reducer class names
		job.setJarByClass(HadoopWordCount.class);
		job.setMapperClass(HadoopMap.class);
		job.setCombinerClass(HadoopReduce.class);
		job.setReducerClass(HadoopReduce.class);
		
		//Setting Input and Output File Format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//the hdfs input and output directory to be fetched from the command line for Hadoop Word Count
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//startTime = System.currentTimeMillis();
		
		job.waitForCompletion(true); // Submit the job, then poll for progress until the job is complete
		
		System.out.println("*****************************************");
		System.out.println("Starting sort Job");
		System.out.println("*****************************************");
		
		Job jobsort = new Job(conf, "Hadoop Word Count Sort");
		jobsort.setOutputKeyClass(IntWritable.class);
		jobsort.setOutputValueClass(Text.class);

		jobsort.setJarByClass(HadoopWordCount.class);
		jobsort.setMapperClass(HadoopMapSort.class);
		jobsort.setCombinerClass(HadoopReduceSort.class);
		jobsort.setReducerClass(HadoopReduceSort.class);
		
		jobsort.setInputFormatClass(TextInputFormat.class);
		jobsort.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(jobsort, new Path(args[2]));
		FileOutputFormat.setOutputPath(jobsort, new Path(args[3]));
		
		jobsort.waitForCompletion(true);

	}
	
	public static class HadoopMapSort extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException
		{
			String line = value.toString();
			line.trim();

			context.write(new IntWritable(Integer.parseInt(value.toString().split("\t")[1])), new Text(value.toString().split("\t")[0]));
			}
		}
	
	public static class HadoopReduceSort extends Reducer<IntWritable, Text, IntWritable, Text>
	{
		public void reduce(IntWritable key, Text values, Context context) throws IOException, InterruptedException
		{
			context.write(key, values);		
	
		}
	}
	
}
