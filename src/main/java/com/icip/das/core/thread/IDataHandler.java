package com.icip.das.core.thread;

import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月7日 下午1:49:57 
 * @update	
 */
public interface IDataHandler extends Cloneable{

	/**
	 * 取数据资源.
	 * 每次处理之前获取数据资源，在serviceDone方法中应该释放掉
	 */
	public void getDataResource(WorkerProxyObject proxy);
	
	/**
	 * 处理数据
	 * @param data
	 */
	public void handleData(WorkerProxyObject proxy);

	/**
	 * 处理之后
	 */
	public void doneWork(WorkerProxyObject proxy);
    
    /**
     * 克隆一个资源对象；
     * @note     克隆出来的资源的工作环境也必须在具体的子类中克隆另一个，才能保证资源使用不冲突；
     */
    public Object clone();
    
}
