package com.lyh.wordcount.searchIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.lyh.wordcount.etl.ETLDriver;

public class SearchIndexDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "D:\\dp\\test\\mapreduce\\searchIndex", "D:\\dp\\test\\mapreduce\\searchIndexOutput" };
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ETLDriver.class);
		job.setMapperClass(SearchIndexMapper.class);
		job.setReducerClass(SearchIndexReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);//注意：因为此处没有Reduce阶段，所以设置最终的输出类型就是Map阶段的输出类型，所以不用单独设置map阶段的输出类型
		job.setOutputValueClass(IntWritable.class);
		//设置reduce并行度为0，此处也可以不用设置，因为没有设置reduce类当然就没有reduce阶段了
		//job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 1 : 0);
	}
}
