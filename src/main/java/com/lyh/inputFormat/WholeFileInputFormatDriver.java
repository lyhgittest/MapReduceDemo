package com.lyh.inputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WholeFileInputFormatDriver {

	public static void main(String[] args) {
		args = new String[] { "D:\\dp\\test\\mapreduce\\WholeFileInputFormat", "D:\\dp\\test\\mapreduce\\WholeFileInputFormatOut"};
		// 1、获取配置信息，创建job任务
		Configuration conf = new Configuration();
		Job job;
		try {
			job = Job.getInstance(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		//设置Mapper类
		job.setMapperClass(WholeFileMapper.class);
		//此处需要设置输入和输出的Format
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// 2、设置文件的路径
		job.setJarByClass(WholeFileInputFormatDriver.class);
		
		// 3、设置输入输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		// 4、设置输入输出路径
		try {
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		// 5、等待执行完成
		boolean completion = false;
		try {
			completion = job.waitForCompletion(true);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(completion ? 1 : 0);
	}
}
