package com.lyh.wordcount.findFriends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class FindFriendsReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger logger = Logger.getLogger(FindFriendsMapper.class);
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		//1、拼接
		for (Text person : values) {
			sb.append(person).append(",");
		}
		context.write(key, new Text(sb.toString()));
	}
}
