package com.icip.das.core.validate;

import com.icip.das.core.exception.SysException;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年2月29日 下午7:12:58 
 * @update	
 */
public interface IDataFilter<T> {

	public abstract T
		handle(T proxy)throws SysException;
	
}
