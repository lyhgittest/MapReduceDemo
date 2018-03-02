package com.lyh.wordcount.sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PhoneSortCombiner extends Reducer<Text, FlowBeanSort, Text, FlowBeanSort> {
	@Override
	protected void reduce(Text key, Iterable<FlowBeanSort> values,
			Context context) throws IOException, InterruptedException {
		for (FlowBeanSort fbs : values) {
			context.write(key, fbs);
		}
	}
}
