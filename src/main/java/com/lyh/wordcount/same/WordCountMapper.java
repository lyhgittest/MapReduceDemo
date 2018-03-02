package com.lyh.wordcount.same;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text k = new Text();
	private Text v = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1、将输入文件转换成String
		String str = value.toString();
		// 2、按照特殊符号(,.""?:!; )进行切割
		String[] words = str.split(" ");
		// 3、将分割的单词输出到reduce中
		for (String word : words) {
			//将单词进行重新排序
			String word2 = WordCountMapper.sortString(word);
			k.set(word2);
			v.set(word);
			context.write(k, v);
		}
		// 根据文件类型获取切片信息
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		// 获取切片的文件名称
		String name = inputSplit.getPath().getName();
		System.out.println(name);

	}

	public static boolean isStrAContainsStrB(String A, String B) {
		boolean flag = false;
		loop1: if (A.length() >= B.length()) {
			A = WordCountMapper.sortString(A);
			B = WordCountMapper.sortString(B);
			if (A.equals(B)) {
				flag = true;
			} else {
				char[] cs1 = A.toCharArray();
				char[] cs2 = B.toCharArray();
				for (int i = 0; i < cs2.length; i++) {
					loop2: for (int j = i; j < cs1.length; j++) {
						if (cs2[i] == cs1[j])
							break loop2;
						else if (j == cs1.length - 1)
							break loop1;
					}
				}
				flag = true;
			}
		}
		return flag;
	}
	/**
	 * 对字符串进行排序
	 * @param a 要排序的字符串
	 * @return
	 */
	public static String sortString(String a) {
		char[] array = a.toCharArray();
		Arrays.sort(array);
		return new String(array);
	}
}
