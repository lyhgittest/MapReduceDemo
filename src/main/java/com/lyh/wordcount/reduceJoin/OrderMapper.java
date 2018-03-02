package com.lyh.wordcount.reduceJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class OrderMapper extends Mapper<LongWritable, Text, Text, TableBean> {
	private Logger logger = Logger.getLogger(OrderMapper.class);
	private String fileName;
	private TableBean tb = new TableBean();
	Text k = new Text();
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// 初始化，获取文件名
		FileSplit split = (FileSplit) context.getInputSplit();
		fileName = split.getPath().getName();
		logger.debug("文件名是debug："+fileName);
	}
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		if (fileName.contains("order")) {//是订单表，订单表中商品与订单是一对多的关系，一个订单下面有多种商品
			tb.setBean(fields[0], fields[1], Integer.parseInt(fields[2]), "", "0");
			k.set(fields[1]);
		}else {//是商品表
			tb.setBean("", fields[0], 0, fields[1], "1");
			k.set(fields[0]);
		}
		context.write(k, tb);
		if (logger.isDebugEnabled()) {
			logger.debug(k+"\t:"+tb);
		}
	}
}
