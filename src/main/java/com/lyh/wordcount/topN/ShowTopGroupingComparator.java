package com.lyh.wordcount.topN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ShowTopGroupingComparator extends WritableComparator {

	/**
	 * 如果对象为空就创建对象，此无参构造方法相当重要，千万不能丢，否则报空指针异常
	 */
	public ShowTopGroupingComparator() {
		super(ShowBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		ShowBean aGbean = (ShowBean) a;
		ShowBean bGbean = (ShowBean) b;
		return aGbean.getSumLoad() > bGbean.getSumLoad() ? -1
				: (aGbean.getSumLoad() < bGbean.getSumLoad() ? 1 : aGbean.getPhoneLoad() > bGbean.getPhoneLoad() ? -1 : 1);
	}
}
