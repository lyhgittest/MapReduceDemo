package com.lyh.wordcount.keyvalueTextFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KeyValueTextInputFormatDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		//设置KeyValueTextInputFormat的分隔符为空格，默认的是\t制表符
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(KeyValueTextInputFormatMapper.class);
		job.setReducerClass(KeyValueTextInputFormatReducer.class);
		
		job.setJarByClass(KeyValueTextInputFormatDriver.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 设置输入格式，这里千万不能弄丢，因为默认的是TextInputFormat.class
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7、等待完成
		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 0 : 1);
	}
}
