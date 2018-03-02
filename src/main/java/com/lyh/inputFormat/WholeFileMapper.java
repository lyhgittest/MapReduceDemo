package com.lyh.inputFormat;

import org.apache.log4j.Logger;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(WholeFileMapper.class);

	private Text k = new Text();
	/**
	 * 相当于是初始化，在每一个mapTask开始的时候都会被调用，此处需要获取文件的路径作为键设置给key输出
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// 1、首先获取分片信息
		FileSplit split = (FileSplit) context.getInputSplit();
		// 2、设置键的值，为文件的路径
		k.set(split.getPath().toString());
	}
	/**
	 * 每一个分片的kv键值对都会执行
	 */
	@Override
	protected void map(NullWritable key, BytesWritable value,Context context)
			throws IOException, InterruptedException {
		if (logger.isInfoEnabled()) {
			logger.info("BytesWritable的内容:"+value);
		}
		context.write(k, value);
	}
}
