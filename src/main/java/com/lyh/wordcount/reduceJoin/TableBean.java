package com.lyh.wordcount.reduceJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Setter
@Getter
public class TableBean implements Writable {

	private String orderId;
	private String pid;
	private int amount;
	private String pname;
	private String flag;//0表示订单表，1表示商品表
	
	@Override
	public void write(DataOutput out) throws IOException {
		// 序列化
		out.writeUTF(orderId);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// 反序列化，与序列化的顺序要一致
		this.orderId = in.readUTF();
		this.pid = in.readUTF();
		this.amount = in.readInt();
		this.pname = in.readUTF();
		this.flag = in.readUTF();
	}
	public void setBean(String orderId,String pid,int amount,String pname,String flag){
		this.orderId = orderId;
		this.pid = pid;
		this.amount = amount;
		this.pname = pname;
		this.flag = flag;
	}
	@Override
	public String toString() {
		return orderId + "\t" + pname + "\t" + amount;
	}
}
