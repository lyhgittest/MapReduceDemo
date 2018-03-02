package com.lyh.wordcount.same;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Text t1 = new Text();
	private StringBuilder sb;
	@Override
	protected void reduce(Text text, Iterable<Text> words, Context context)
			throws IOException, InterruptedException {
		sb = new StringBuilder();
		for (Text word : words) {
			sb.append(word.toString()).append(" ");
		}
		t1.set(sb.toString());
		context.write(NullWritable.get(), t1);
	}
}
