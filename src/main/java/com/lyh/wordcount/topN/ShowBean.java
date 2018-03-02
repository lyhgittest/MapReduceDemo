package com.lyh.wordcount.topN;

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
public class ShowBean implements WritableComparable<ShowBean> {

	private Long sumLoad;
	private Long phoneLoad;
	private Long upLoad;
	private Long downLoad;

	@Override
	public void write(DataOutput out) throws IOException {
		// 序列化
		out.writeLong(sumLoad);
		out.writeLong(phoneLoad);
		out.writeLong(upLoad);
		out.writeLong(downLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// 反序列化
		this.sumLoad = in.readLong();
		this.phoneLoad = in.readLong();
		this.upLoad = in.readLong();
		this.downLoad = in.readLong();
	}

	@Override
	public int compareTo(ShowBean o) {//此处决定排序的顺序，此处安装降序排序
		return this.sumLoad - o.sumLoad > 0 ? -1
				: this.sumLoad - o.sumLoad < 0 ? 1 : this.phoneLoad - this.phoneLoad > 0 ? -1 : 1;
	}

	public void setBean(Long phoneLoad, Long downLoad, Long upLoad, Long sumLoad) {
		this.phoneLoad = phoneLoad;
		this.downLoad = downLoad;
		this.upLoad = upLoad;
		this.sumLoad = sumLoad;
	}

	@Override
	public String toString() {
		return this.phoneLoad + "\t" + this.downLoad + "\t" + this.upLoad + "\t" + this.sumLoad;
	}
}
