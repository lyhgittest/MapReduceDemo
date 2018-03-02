package com.lyh.wordcount.etl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *  对web访问日志中的各字段识别切分
去除日志中不合法的记录
根据统计需求，生成各类访问请求过滤数据

 * @author barry
 *
 */
public class ETLMapperComplex extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Text k = new Text();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		LogBean logBean = this.parseLog(value.toString());
		if (!logBean.isValid()) {
			return ;
		}
		k.set(logBean.toString());
		context.write(k, NullWritable.get());
	}
	
	private LogBean parseLog(String line){
		LogBean logBean = new LogBean();
		String[] fields = line.split(" ");
		if (fields.length>11) {
			//封装数据
			logBean.setBean(fields[0], fields[1], fields[3].substring(1), 
					fields[6], fields[8], fields[9], fields[10]);
			if (fields.length>12) {
				logBean.setHttp_user_agent(fields[11]+" "+fields[12]);
			}else {
				logBean.setHttp_user_agent(fields[11]);
			}
			if (Integer.parseInt(logBean.getStatus())>=400) {
				logBean.setValid(false);
			}
		}else {
			logBean.setValid(false);
		}
		return logBean;
	}
}
