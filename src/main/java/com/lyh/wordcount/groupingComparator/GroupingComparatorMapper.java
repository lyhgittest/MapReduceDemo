package com.lyh.wordcount.groupingComparator;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 将下面的信息按照订单号分区显示每个订单号的最大价格
 订单号	商品号	价格
 0000001	Pdt_01	222.8
0000002	Pdt_06	722.4
0000001	Pdt_05	25.8
0000003	Pdt_01	222.8
0000003	Pdt_01	33.8
0000002	Pdt_03	522.8
0000002	Pdt_04	122.4
 *
 */
public class GroupingComparatorMapper extends Mapper<LongWritable, Text, GroupOrderBean, NullWritable> {

	private GroupOrderBean gob = new GroupOrderBean();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//1、获取一行数据
		String string = value.toString();
		//2、切割
		String[] fields = string.split("\t");
		//3、封装
		gob.setOrderId(Integer.parseInt(fields[0]));
		gob.setPrice(Double.parseDouble(fields[2]));
		//4、输出
		context.write(gob, NullWritable.get());
	}
}
