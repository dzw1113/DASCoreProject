package com.icip.das.core.validate;


/** 
 * @Description: TODO
 * @author  
 * @date 2016年3月18日 下午2:25:13 
 * @update	
 */
public interface ICustomerHandler<T> {

	public abstract T handle(T proxy);
	
}
