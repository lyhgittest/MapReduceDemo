package com.lyh.wordcount.topN;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShowTop10Mapper extends Mapper<LongWritable, Text, ShowBean, NullWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		ShowBean bean = new ShowBean();
		bean.setBean(Long.parseLong(fields[0]), Long.parseLong(fields[1]), 
				Long.parseLong(fields[2]), Long.parseLong(fields[3]));
		context.write(bean, NullWritable.get());
	}
}
