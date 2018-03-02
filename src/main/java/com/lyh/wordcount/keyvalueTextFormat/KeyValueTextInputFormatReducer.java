package com.lyh.wordcount.keyvalueTextFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KeyValueTextInputFormatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private int count;
	private IntWritable v = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		v.set(count);
		context.write(key, v);
	}
}
