package com.bigdata.acadgild;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenerateInputForAssignment6_1 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration con = new Configuration();
		Job job = new Job(con);
		job.setJarByClass(GenerateInputForAssignment6_1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(GMapper.class);
		
		job.setReducerClass(GReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
}
	
	private static class GMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final String NA = "NA"; 
		public void map(LongWritable key, Text value, Context context ) throws IOException,InterruptedException
		{
			String[] values = value.toString().split("\\|");
			if ( !NA.equals(values[0]) &&  !NA.equals(values[1]))  {
				context.write(new Text(values[0]), new IntWritable(1));
			}
		}
	}
	
	private static class GReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private Integer minValue = Integer.MIN_VALUE;
		private IntWritable total = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			
			Integer totalUnits = 0;
			for ( IntWritable value : values ) 
			{
				if ( value.get() > minValue )
				{
					totalUnits+=value.get();
				}
			}
			total.set(totalUnits);
			context.write(key, total);
		}
	}
	
	
}
