package com.lyh.wordcount.etl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
/**
 * 用于记录日志中个字段的数据
 * @author barry
 *
 */

@NoArgsConstructor
@Setter
@Getter
public class LogBean{

	private String remote_addr;// 记录客户端的ip地址
	private String remote_user;// 记录客户端用户名称,忽略属性"-"
	private String time_local;// 记录访问时间与时区
	private String request;// 记录请求的url与http协议
	private String status;// 记录请求状态；成功是200
	private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
	private String http_referer;// 用来记录从那个页面链接访问过来的
	private String http_user_agent;// 记录客户浏览器的相关信息

	private boolean valid = true;// 判断数据是否合法

	public void setBean(String remote_addr,String remote_user,String time_local,
			String request,String status,String body_bytes_sent,
			String http_referer){
		this.remote_addr = remote_addr;
		this.remote_user =remote_user;
		this.time_local=time_local;
		this.request=request;
		this.status=status;
		this.body_bytes_sent=body_bytes_sent;
		this.http_referer=http_referer;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.valid);
		sb.append("\001").append(this.remote_addr);
		sb.append("\001").append(this.remote_user);
		sb.append("\001").append(this.time_local);
		sb.append("\001").append(this.request);
		sb.append("\001").append(this.status);
		sb.append("\001").append(this.body_bytes_sent);
		sb.append("\001").append(this.http_referer);//"\001"表示SOH
		sb.append("\001").append(this.http_user_agent);
		
		return sb.toString();
	}

}
