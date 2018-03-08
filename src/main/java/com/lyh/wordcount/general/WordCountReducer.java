package com.lyh.wordcount.general;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private int sum;
	private IntWritable v = new IntWritable();

	@Override
	protected void reduce(Text text, Iterable<IntWritable> words, Context context)
			throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable word : words) {
			sum += word.get();
		}
		v.set(sum);
		context.write(text, v);
	}

}
