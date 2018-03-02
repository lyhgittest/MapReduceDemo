package com.lyh.wordcount.groupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupComparatorPartition extends Partitioner<GroupOrderBean, NullWritable> {

	@Override
	public int getPartition(GroupOrderBean key, NullWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		return (key.getOrderId() & Integer.MAX_VALUE) % numPartitions;
	}

}
