package com.lyh.wordcount.searchIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class SearchIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static final Logger logger = Logger.getLogger(SearchIndexReducer.class);
	
	private IntWritable v = new IntWritable();
	private int count;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		count = 0;
		for (IntWritable text : values) {
			count+=text.get();
		}
		v.set(count);
		context.write(key, v);
		
	}
}
