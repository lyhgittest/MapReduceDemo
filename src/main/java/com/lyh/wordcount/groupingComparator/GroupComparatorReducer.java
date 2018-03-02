package com.lyh.wordcount.groupingComparator;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupComparatorReducer extends Reducer<GroupOrderBean, NullWritable, GroupOrderBean, NullWritable> {

	@Override
	protected void reduce(GroupOrderBean key, Iterable<NullWritable> values,Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
