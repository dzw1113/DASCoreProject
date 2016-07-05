package com.icip.das.core;

/** 
 * @Description: 数据异步处理之后的回调接口
 * @author  yzk
 * @date 2016年3月7日 下午3:47:08 
 * @update	
 */
public interface IDataCallback {

	public void callback(Object result, Exception e);

}
