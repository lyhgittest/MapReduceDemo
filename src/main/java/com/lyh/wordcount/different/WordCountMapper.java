package com.lyh.wordcount.different;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text k = new Text();
	private IntWritable v = new IntWritable(1);
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		CombineFileSplit split = (CombineFileSplit) context.getInputSplit();
		Path[] paths = split.getPaths();
		for (Path path : paths) {
			System.out.println("split路径："+path.toString());
		}
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//1、将输入文件转换成String
		String str = value.toString();
		//2、按照特殊符号(,.""?:!; )进行切割
		String[] words = str.split("[,.\"\"?:!;'() ]+");
		//String[] words = str.split(" ");
		//3、将分割的单词输出到reduce中
		for (String word : words) {
			k.set(word);
			context.write(k, v);
		}
		//System.out.println(key.toString() +" " + value);
	}
}
