package com.lyh.wordcount.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhoneSortDriver {

	public static void main(String[] args) throws Exception {
		// 1、设置配置获取任务对象封装任务job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 2、指定本地程序的jar包所在路径
		job.setJarByClass(PhoneSortDriver.class);
		// 3、设置mapper和reducer类
		job.setMapperClass(PhoneSortMapper.class);
		job.setReducerClass(PhoneSortReducer.class);
		// 4、设置mapper和reducer类的输入和输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBeanSort.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBeanSort.class);
		//7、设置combiner合并器
		//job.setCombinerClass(PhoneSortCombiner.class);
		job.setCombinerClass(PhoneSortReducer.class);
		// 5、设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 6、等待完成
		boolean completion = job.waitForCompletion(true);// 这里面进去有submit方法
		System.exit(completion ? 0 : 1);
	}
}
