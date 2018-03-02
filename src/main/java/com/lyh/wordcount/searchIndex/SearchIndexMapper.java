package com.lyh.wordcount.searchIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class SearchIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Logger logger = Logger.getLogger(SearchIndexReducer.class);

	private String fileName = null;
	private Text k = new Text();
	private IntWritable v = new IntWritable(1);

	/**
	 * 获取文件名
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) context.getInputSplit();
		fileName = split.getPath().toString();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(" ");
		for (String string : fields) {
			k.set(string + "--" + fileName.substring(fileName.lastIndexOf("/") + 1));
			context.write(k, v);
		}
		if (logger.isInfoEnabled()) {
			logger.info(fileName.substring(fileName.lastIndexOf("/") + 1) + "--");
		}
	}

}
