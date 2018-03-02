package com.lyh.wordcount.topN;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ShowTop10Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "D:\\dp\\test\\mapreduce\\phonecountOut", "D:\\dp\\test\\mapreduce\\phonecountOutPut" };
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ShowTop10Driver.class);
		job.setMapperClass(ShowTop10Mapper.class);
		job.setReducerClass(ShowTop10Reducer.class);
		job.setMapOutputKeyClass(ShowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(ShowBean.class);
		job.setOutputValueClass(NullWritable.class);
		//设置分区和reduceTask任务数量
		//job.setPartitionerClass(GroupComparatorPartition.class);
		//job.setNumReduceTasks(3);
		//设置GroupingComparator
		job.setGroupingComparatorClass(ShowTopGroupingComparator.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 1 : 0);
	}
}
