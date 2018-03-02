package com.lyh.wordcount.findFriends;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？
 * 先找出各个人都属于哪些人的好友
 * @author barry
 *
 */
public class FindFriendsMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final Logger logger = Logger.getLogger(FindFriendsMapper2.class);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		String friend = line[0];
		String[] persons = line[1].split(",");
		Arrays.sort(persons);
		for (int i = 0; i < persons.length-1; i++) {
			for (int j = i+1; j < persons.length; j++) {
				context.write(new Text(persons[i]+"--"+persons[j]+":"), new Text(friend));
			}
		}
	}
}
