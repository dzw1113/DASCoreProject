package com.icip.framework.test;

import java.io.Serializable;

/**
 * 定义元数据
 */
public class MyMata  implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private Long beginPoint ;//事务开始位置
	
	private int  num; //batch 的tuple个数

	public Long getBeginPoint() {
		return beginPoint;
	}

	public void setBeginPoint(Long beginPoint) {
		this.beginPoint = beginPoint;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	@Override
	public String toString() {
		return "事务：开始位置:"+getBeginPoint()+", 发送数据大小"+getNum();
	}
	
	
}