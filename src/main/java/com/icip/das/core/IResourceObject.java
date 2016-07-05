package com.icip.das.core;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月7日 下午1:16:15 
 * @update	
 */
public interface IResourceObject {

	/**
     * 资源的初始化方法
     */
    public void   initilize();
    
    /**
     * 释放资源的工作环境
     */
    public void   release();

    /**
     * 克隆一个资源对象；
     * @note     克隆出来的资源的工作环境也必须在具体的子类中克隆另一个，才能保证资源使用不冲突；
     */
    public Object clone();
    
}
