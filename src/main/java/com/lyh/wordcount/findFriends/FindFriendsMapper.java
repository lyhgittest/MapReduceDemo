package com.lyh.wordcount.findFriends;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？ 先找出各个人都属于哪些人的好友
 * 
 * @author barry
 *
 */
public class FindFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Logger logger = Logger.getLogger(FindFriendsMapper.class);
	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(":");
		String person = line[0];
		String[] friends = line[1].split(",");
		if (logger.isDebugEnabled()) {
			logger.info(line);
		}

		for (String friend : friends)
			context.write(new Text(friend), new Text(person));
	}
}
