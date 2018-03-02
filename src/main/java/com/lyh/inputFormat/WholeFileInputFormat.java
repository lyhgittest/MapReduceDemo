package com.lyh.inputFormat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	/**
	 * 判断要不要对文件进行分片，此处直接按照整个文件进行处理，所以不用分片，为false
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	/**
	 * 在该方法中实现业务逻辑，为分区创建RecordReader，
	 */
	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		WholeFileRecordReader wfr = new WholeFileRecordReader();
		wfr.initialize(split, context);
		return wfr;
	}

}
