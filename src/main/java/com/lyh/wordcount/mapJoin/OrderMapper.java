package com.lyh.wordcount.mapJoin;

import org.apache.log4j.Logger;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(OrderMapper.class);

	
	private Map<String, String> map = new HashMap<>();
	Text k = new Text();
	/**
	 * 将文件读取到缓存集合中
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//获取缓存文件
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("d:/dp/pd.txt"), "UTF-8"));
		//BufferedReader br = new BufferedReader(new FileReader("pd.txt"));
		String line;
		while (StringUtils.isNotEmpty(line=br.readLine())) {
			//切割内容
			String[] fields = line.split("\t");
			//将内容放到map中
			map.put(fields[0], fields[1]);
		}
		//关闭流资源
		br.close();
	}
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//获取内容并截取
		String line = value.toString();
		String[] fields = line.split("\t");
		//获取pid
		String pid = fields[1];
		//获取pname
		String pname = map.get(pid);
		if (logger.isDebugEnabled()) {
			logger.debug(line+","+pname+","+pid);
			logger.debug(map.toString());
		}
		//封装数据
		k.set(line+"\t" +pname);
		//输出
		context.write(k, NullWritable.get());
	}
}
