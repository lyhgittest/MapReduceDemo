package com.lyh.wordcount.etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 清理数据：去除日志中字段长度小于等于11的日志。
 * 
 * @author barry
 *
 */
public class ETLDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "D:\\dp\\test\\ETL", "D:\\dp\\test\\ETLComplexOut" };
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ETLDriver.class);
		//job.setMapperClass(ETLMapperSimple.class);//简单解析
		job.setMapperClass(ETLMapperComplex.class);//复杂解析
		job.setOutputKeyClass(Text.class);//注意：因为此处没有Reduce阶段，所以设置最终的输出类型就是Map阶段的输出类型，所以不用单独设置map阶段的输出类型
		job.setOutputValueClass(NullWritable.class);
		//设置reduce并行度为0，此处也可以不用设置，因为没有设置reduce类当然就没有reduce阶段了
		//job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 1 : 0);
	}
}
