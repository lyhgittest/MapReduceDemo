package com.lyh.wordcount.NLineInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineInputFormatDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		// 设置每个切片InputSplit中划分三条记录,一共有12条记录
        NLineInputFormat.setNumLinesPerSplit(job, 2);

		job.setMapperClass(NLineInputFormatMapper.class);
		job.setReducerClass(NLineInputFormatReducer.class);
		
		job.setJarByClass(NLineInputFormatDriver.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 设置输入格式，这里千万不能弄丢，因为默认的是TextInputFormat.class
		job.setInputFormatClass(NLineInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7、等待完成
		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 0 : 1);
	}
}
