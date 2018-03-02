package com.lyh.wordcount.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class PhoneSortMapper extends Mapper<LongWritable, Text, Text, FlowBeanSort> {

	private Text phoneNum = new Text();
	private FlowBeanSort fb = new FlowBeanSort();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//1、获取每一行的数据
		String data = value.toString();
		//2、将数据按tab键拆分
		String[] datas = data.split("\t");
		//3、获取手机号
		phoneNum.set(datas[0]);
		//4、获取上行流量和下行流量并封装
		long upLoad = Long.parseLong(datas[datas.length-3]);
		long downLoad = Long.parseLong(datas[datas.length-2]);
		fb.setUpFlow(upLoad);
		fb.setDownFlow(downLoad);
		fb.setSumFlow(upLoad+downLoad);
		//5、将数据输出到reduce
		context.write(phoneNum, fb);
		//System.out.println("电话号码："+phoneNum);
	}
}
