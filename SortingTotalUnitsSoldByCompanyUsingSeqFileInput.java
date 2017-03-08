package com.bigdata.acadgild;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SortingTotalUnitsSoldByCompanyUsingSeqFileInput {

	public static void main(String[] args) throws Exception {
		
		Configuration con = new Configuration();
		Job job = new Job(con);
		job.setJarByClass(SortingTotalUnitsSoldByCompanyUsingSeqFileInput.class);
		
		job.setMapperClass(GMapper.class);
		job.setMapOutputKeyClass(Company.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(GReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}
	
	private static class GMapper extends Mapper<Text, IntWritable, Company, IntWritable>
	{
		private Company company = new Company();
		
		public void map(Text key, IntWritable value, Context context ) throws IOException,InterruptedException
		{
			company.setName(key.toString());
			company.setUnitsSold(value.get());
			context.write(company, value);
			
		}
	}
	
	private static class GReducer extends Reducer<Company, IntWritable, Text, IntWritable>
	{
		private Text t = new Text();
		private IntWritable us = new IntWritable();
		
		public void reduce(Company key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			Integer count = 0;
			t.set(key.getName());
			for ( IntWritable value : values ) 
			{
				count += value.get();
			}
			us.set(count);
			context.write(t, us);
		}
	}
	
	private static class Company implements WritableComparable<Company>
	{
		private String name;
		private Integer unitsSold;

		public Company() {}
		
		@Override
		public void readFields(DataInput arg0) throws IOException {
			name = arg0.readUTF();
			unitsSold = arg0.readInt();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			arg0.writeUTF(name);
			arg0.writeInt(unitsSold);
		}

		@Override
		public int compareTo(Company o) {
			
			int u = unitsSold.compareTo(o.getUnitsSold());
			return -1 * u;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public void setUnitsSold(Integer unitsSold) {
			this.unitsSold = unitsSold;
		}
		
		public String getName() {
			return name;
		}
		
		public Integer getUnitsSold() {
			return unitsSold;
		}
	}

}
