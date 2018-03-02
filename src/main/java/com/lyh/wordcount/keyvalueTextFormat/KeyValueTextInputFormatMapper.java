package com.lyh.wordcount.keyvalueTextFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 统计输入文件中每一行的第一个单词相同的行数。
 * @author barry
 *
 */
public class KeyValueTextInputFormatMapper extends Mapper<Text, Text, Text, IntWritable>{

	private Text k = new Text();
	private IntWritable v = new IntWritable(1);
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		k.set(key);
		context.write(k, v);
	}
}
