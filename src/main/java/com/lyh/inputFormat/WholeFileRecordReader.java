package com.lyh.inputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

	// 定义一个fileSplit,封装了文件的路径、大小、起始位置等信息
	private FileSplit split;
	// 定义输出内容的字节类型
	private BytesWritable bytesWritable = new BytesWritable();
	// 定义配置
	private Configuration conf;
	// 定义一个Boolean类型，判断业务程序是否完成
	private boolean isProcessed = false;

	/**
	 * 初始化自定义的inputFormat，整个业务处理中只会执行一次
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.conf = context.getConfiguration();
		this.split = (FileSplit) split;
	}

	/**
	 * 读取下一个键值对，如果读取到了下一个键值对就返回true否则返回false
	 * 此处获取文件内容，并将内容写到inputformat中此处是具体的业务逻辑实现,每一次分片就会执行该方法
	 */
	@Override
	public boolean nextKeyValue() throws InterruptedException {
		if (!isProcessed) {
			// 业务没有处理完成，开始处理业务
			// 1、创建一个输入流读取文件,通过文件系统来创建，所以需要先获取文件系统
			FileSystem fs = null;
			FSDataInputStream fis = null;
			try {
				fs = FileSystem.get(conf);
				fis = fs.open(split.getPath());
				// 2、通过IOUtils将文件内容读入到fis中,读取整个分片文件，需要先创建一个字节数组作为缓冲区
				// 创建字节缓冲区，通过FileSplit获取分片文件的大小作为每个缓冲区的大小
				byte[] bytes = new byte[(int) split.getLength()];
				IOUtils.readFully(fis, bytes, 0, (int) split.getLength());
				// 3、通过BytesWritable设置输出文件的内容，输出文件
				bytesWritable.set(bytes, 0, (int) split.getLength());
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				// 4、关闭流资源
				if (fis != null) {
					try {
						fis.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			isProcessed = true;
			return true;
		}
		return false;

	}
	//获取当前的键，此处为空串
	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return NullWritable.get();
	}
	//获取当前键值对的值
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println(bytesWritable);
		return bytesWritable;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return isProcessed ? 1 : 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
