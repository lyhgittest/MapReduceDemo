package com.lyh.wordcount.general;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 使用默认的inputformat,普通的wordcount案例
 * 
 * @author barry
 *
 */
public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		args = new String[]{"D:/dp/test/mapreduce/wordcountinput","D:/dp/test/mapreduce/wordcountoutput"};
		// 1、获取配置文件创建job任务
		Configuration conf = new Configuration();
		// 注意：此处是通过get方法获取实例，不是new出来的。
		Job job = Job.getInstance(conf);
		// 如果不设置InputFormatClass的话默认是TextInputFomat

		// job.setInputFormatClass(KeyValueTextInputFormat.class);
		// 2、设置本地程序的jar包所在路径
		job.setJarByClass(WordCountDriver.class);
		// 3、设置mapper和reducer类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		// job.setCombinerClass(WordCountReducer.class);
		// 4、设置Mapper的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 5、设置最终的输出结果类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 6、设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7、等待完成
		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 0 : 1);
	}
}
