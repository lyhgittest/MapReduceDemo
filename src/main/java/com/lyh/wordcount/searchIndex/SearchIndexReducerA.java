package com.lyh.wordcount.searchIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class SearchIndexReducerA extends Reducer<Text, Text, Text, Text> {

	private static final Logger logger = Logger.getLogger(SearchIndexMapperA.class);
	private Text v = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for (Text text : values) {
			sb.append("\t").append(text.toString());
		}
		v.set(sb.toString());
		context.write(key, v);
	}
}
