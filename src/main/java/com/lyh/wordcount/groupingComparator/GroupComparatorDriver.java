package com.lyh.wordcount.groupingComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupComparatorDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(GroupComparatorDriver.class);
		job.setMapperClass(GroupingComparatorMapper.class);
		job.setReducerClass(GroupComparatorReducer.class);
		job.setMapOutputKeyClass(GroupOrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(GroupOrderBean.class);
		job.setOutputValueClass(GroupOrderBean.class);
		//设置分区和reduceTask任务数量
		//job.setPartitionerClass(GroupComparatorPartition.class);
		//job.setNumReduceTasks(3);
		//设置GroupingComparator
		job.setGroupingComparatorClass(OrderGroupingComparator.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 1 : 0);
	}
}
