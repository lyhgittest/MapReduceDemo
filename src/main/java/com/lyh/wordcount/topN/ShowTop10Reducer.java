package com.lyh.wordcount.topN;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ShowTop10Reducer extends Reducer<ShowBean, NullWritable, ShowBean, NullWritable> {
	private int count = 0;

	@Override
	protected void reduce(ShowBean key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		++count;
		if (count > 10)
			return;
		context.write(key, NullWritable.get());
	}
}
