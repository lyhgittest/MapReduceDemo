package com.lyh.wordcount.etl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ETLMapperSimple extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//1、获取一行数据
		String line = value.toString();
		//2、判断数据长度是否小于等于11，如果是去除并统计,注意此处是字段的个数，所以需要转换成数组判断
		String[] fields = line.split(" ");
		if (fields.length<=11) {
			context.getCounter("Counter", "badNews").increment(1);
		}else {
			context.getCounter("Counter", "goodNews").increment(1);
			context.write(value, NullWritable.get());
		}
	}
}
