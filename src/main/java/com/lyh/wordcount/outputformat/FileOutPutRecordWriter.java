package com.lyh.wordcount.outputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FileOutPutRecordWriter extends RecordWriter<Text, NullWritable> {

	private FSDataOutputStream atguiguOut = null;
	private FSDataOutputStream otherOut = null;
	public FileOutPutRecordWriter() {
	}

	public FileOutPutRecordWriter(TaskAttemptContext job) {
		//初始化资源
		Configuration conf = job.getConfiguration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			atguiguOut = fs.create(new Path("D:\\dp\\test\\atguigu.log"));
			otherOut = fs.create(new Path("D:\\dp\\test\\others.log"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		// 输出逻辑
		String values = key.toString();
		if (values.contains("atguigu")) {
			atguiguOut.write(values.getBytes());
		}else {
			otherOut.write(values.getBytes());
			otherOut.write("\r\n".getBytes());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// 关闭流资源
		otherOut.close();
		atguiguOut.close();
	}

}
