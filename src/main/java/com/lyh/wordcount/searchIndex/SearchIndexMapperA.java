package com.lyh.wordcount.searchIndex;

import org.apache.log4j.Logger;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SearchIndexMapperA extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Logger logger = Logger.getLogger(SearchIndexMapperA.class);
	private Text k = new Text();
	private Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		k.set(line.substring(0, line.indexOf("-")));
		v.set(line.substring(line.lastIndexOf("-")+1));
		if (logger.isInfoEnabled()) {
			logger.info(line.substring(0, line.indexOf("-"))+"............"+line.substring(line.lastIndexOf("-")+1));
		}
		context.write(k, v);
	}
}
