package com.lyh.wordcount.groupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 对reduce阶段的订单号进行分组，只排序价格
 * 
 * @author barry
 *
 */
public class OrderGroupingComparator extends WritableComparator {

	/**
	 * 如果对象为空就创建对象，此无参构造方法相当重要，千万不能丢，否则报空指针异常
	 */
	public OrderGroupingComparator() {
		super(GroupOrderBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		GroupOrderBean aGbean = (GroupOrderBean) a;
		GroupOrderBean bGbean = (GroupOrderBean) b;
		return aGbean.getOrderId() > bGbean.getOrderId() ? 1
				: (aGbean.getOrderId() < bGbean.getOrderId() ? -1 : aGbean.getPrice() > bGbean.getPrice() ? 1 : -1);
	}
}
