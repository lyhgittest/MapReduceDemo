package com.lyh.wordcount.NLineInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NLineInputFormatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable v = new IntWritable();
	private int count;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> words,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		count = 0;
		for (IntWritable word : words) {
			count+=word.get();
		}
		v.set(count);
		context.write(key, v);
	}
}
