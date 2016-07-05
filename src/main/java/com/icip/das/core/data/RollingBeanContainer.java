package com.icip.das.core.data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** 
 * @Description:	严格控制轮询时间，不然并发轮询同一张表时会出现重复数据 
 * 					只在定时任务触发时清空对应的bean
 * @author  
 * @date 2016年4月8日 下午2:01:55 
 * @update	
 */
public class RollingBeanContainer {

	public static ConcurrentMap<String, TableRollingBean> ROLLING_CONFIG = new ConcurrentHashMap<String, TableRollingBean>();
	
	private static Object setLock = new Object();
	
	private static Object removeLock = new Object();

	//严格线程安全
	public synchronized static void updateRollingBean(String tableName,TableRollingBean bean){
		ROLLING_CONFIG.put(tableName, bean);
	}
	
	public static TableRollingBean getRollingBean(String tableName){
		TableRollingBean bean = ROLLING_CONFIG.get(tableName);
		return bean;
	}
	
	/**
	 *	 ROLLING_CONFIG中有不添加
	 */
	public static void setRollingBean(String tableName,TableRollingBean bean){
		if(!ROLLING_CONFIG.containsKey(tableName)){
			synchronized(setLock){
				if(!ROLLING_CONFIG.containsKey(tableName)){
					ROLLING_CONFIG.put(tableName, bean);
				}
			}
		}
	}
	
	public static void removeRollingBean(String tableName){
		if(ROLLING_CONFIG.containsKey(tableName)){
			synchronized(removeLock){
				if(ROLLING_CONFIG.containsKey(tableName)){
					RollingBeanContainer.removeRollingBean(tableName);
				}
			}
		}
	}
	
}
