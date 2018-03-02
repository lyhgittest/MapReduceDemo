package com.lyh.wordcount.reduceJoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class OrderReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
	private Logger logger = Logger.getLogger(OrderReducer.class);
	
	@Override
	protected void reduce(Text key, Iterable<TableBean> values,Context context)
			throws IOException, InterruptedException {
		List<TableBean> beans  = new ArrayList<>();
		TableBean pbean = new TableBean();
		for (TableBean tableBean : values) {
			if (tableBean.getFlag().equals("0")) {//订单表
				TableBean listbean = new TableBean();
				try {
					BeanUtils.copyProperties(listbean, tableBean);
					if (logger.isDebugEnabled()) {
						logger.debug(key+"\t2:"+beans);
					}
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
				beans.add(listbean);
			}else {
				try {
					BeanUtils.copyProperties(pbean, tableBean);
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}
		for (TableBean tableBean : beans) {
			tableBean.setPname(pbean.getPname());
			if (logger.isDebugEnabled()) {
				logger.debug(tableBean);
			}
			context.write(tableBean, NullWritable.get());
		}
	}
}
