package com.lyh.wordcount.NLineInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineInputFormatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text k = new Text();
	private IntWritable v = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split("");
		for (int i = 0; i < words.length; i++) {
			k.set(words[i]);
			context.write(k, v);
		}
	}
}
