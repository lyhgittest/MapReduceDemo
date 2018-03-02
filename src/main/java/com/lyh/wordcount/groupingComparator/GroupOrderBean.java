package com.lyh.wordcount.groupingComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Setter
@Getter
public class GroupOrderBean implements WritableComparable<GroupOrderBean> {

	private int orderId;
	private double price;

	@Override
	public void write(DataOutput out) throws IOException {
		// 序列化
		out.writeInt(orderId);
		out.writeDouble(price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// 反序列化，顺序不能出错
		this.orderId = in.readInt();
		this.price = in.readDouble();
	}

	@Override
	public int compareTo(GroupOrderBean o) {
		// 比较，会有二次比较,如果订单号相同了就比较价格，大于为负数是降序，为正数是升序
		return this.orderId > o.orderId ? -1 : (this.orderId < o.orderId ? 1 : this.price >= o.price ? -1 : 1);
	}

	@Override
	public String toString() {
		return this.orderId + "\t" + this.price;
	}
}
