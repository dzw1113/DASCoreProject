package com.icip.das.core.data;

import java.util.Date;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月8日 下午1:56:18 
 * @update	
 */
public class TableRollingBean {

	/** 表名    */
	private String tableName;
	
	/** 最新轮询时间   */
	private Date   lastRollingTime;

	/** 下次轮询时间   */
	private Date nextRollingTime;
	
	/** 临时时间 没有异常时清空,发生异常时根据此时间回滚  */
	private volatile Date temp;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Date getLastRollingTime() {
		return lastRollingTime;
	}

	public void setLastRollingTime(Date lastRollingTime) {
		this.lastRollingTime = lastRollingTime;
	}

	public Date getNextRollingTime() {
		return nextRollingTime;
	}

	public void setNextRollingTime(Date nextRollingTime) {
		this.nextRollingTime = nextRollingTime;
	}

	public Date getTemp() {
		return temp;
	}

	public void setTemp(Date temp) {
		this.temp = temp;
	}
	
}
