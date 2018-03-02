package com.lyh.wordcount.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(OrderDriver.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 1 : 0);
	}
}
