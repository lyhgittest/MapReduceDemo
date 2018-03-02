package com.lyh.wordcount.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FileOutPutFormatDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(FileOutPutMapper.class);
		//job.setReducerClass(KeyValueTextInputFormatReducer.class);
		
		job.setJarByClass(FileOutPutFormatDriver.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 设置输入格式，这里千万不能弄丢，因为默认的是TextInputFormat.class
		job.setOutputFormatClass(MyOutPutFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7、等待完成
		boolean completion = job.waitForCompletion(true);
		System.exit(completion ? 0 : 1);
	}

}
