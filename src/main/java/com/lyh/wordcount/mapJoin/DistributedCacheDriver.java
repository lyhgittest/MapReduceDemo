package com.lyh.wordcount.mapJoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * map段join适用于有大表和小表的情况，可以防止数据倾斜，减少reduce端压力，不需要reduce阶段
 * @author barry
 *
 */
public class DistributedCacheDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DistributedCacheDriver.class);
		job.setMapperClass(OrderMapper.class);
		//job.setReducerClass(OrderReducer.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(TableBean.class);
		//如果没有reduce阶段后设置最终的输出就相当于设置了map的输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//加载缓存数据，这一步会将缓存文件发给每一个map
		//job.addCacheFile(new URI("file:///D:/dp/test/mapreduce/cache/pd.txt"));
		job.addCacheFile(new URI("file:///d:/dp/pd.txt"));
		//map端join的逻辑不需要reduce阶段，设置reduceTask数量为0
		job.setNumReduceTasks(0);
		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 1 : 0);
	}
}
