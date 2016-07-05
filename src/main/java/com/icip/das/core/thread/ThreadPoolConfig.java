package com.icip.das.core.thread;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月7日 下午8:42:19 
 * @update	
 */
public class ThreadPoolConfig implements java.io.Serializable{

	private static final long serialVersionUID = 2092835945780625847L;

	/** 最大空闲资源时间，单位毫秒 **/
	private final static long MAX_IDLE_TIME_DEFAULT         = 600 * 1000;

	/** 资源池步长增加的默认值 **/
	private final static int  STEP_DEFAULT                  = 10;

	/** 从资源池获取资源默认持续时间，单位毫秒 **/
	private final static int  CHECKOUT_DURATIVE_TIME_DEFAULT = 6 * 1000 * 3600;
	
	/** 缺省的最大资源池数量   */
	private final static int  MAX_POOL_SIZE_DEFAULT          = 100;

	/** 缺省的最大空闲资源数  */
	private final static int  MAX_IDLE_POOL_SIZE_DEFAULT     = 10;

	/** 资源池的最小值 **/
	private volatile int      minPoolSize;

	/** 资源池的最大值 **/
	private volatile int      maxPoolSize;

	/** 资源池每次增加的步长 **/
	private volatile int      step;

	/** 资源池的最大空闲资源数 **/
	private volatile int      maxIdlePoolSize;

	/** 空闲资源的存活时间 **/
	private volatile long     keepAliveTime;


	/** 从资源池获取资源的持续时间 **/
	private volatile long     checkOutDurativeTime;

	/**
	 * 构造函数.<br>
	 */
	public ThreadPoolConfig(){
		this( 0,  MAX_POOL_SIZE_DEFAULT, MAX_IDLE_POOL_SIZE_DEFAULT, MAX_IDLE_TIME_DEFAULT,
				 STEP_DEFAULT, CHECKOUT_DURATIVE_TIME_DEFAULT );
	}

	/**
	 * 构造函数.<br>
	 * @param minPoolSize
	 *            资源池最小值
	 * @param maxPoolSize
	 *            资源池最大值
	 */
	public ThreadPoolConfig(int minPoolSize, int maxPoolSize) {
		this();
		this.setMinPoolSize(minPoolSize);
		this.setMaxPoolSize(maxPoolSize);
	}

	/**
	 * 构造函数.<br>
	 * @param minPoolSize
	 *            资源池最小值
	 * @param maxPoolSize
	 *            资源池最大值
	 * @param maxIdlePoolSize
	 *            允许最大空闲资源大小
	 * @param keepAliveTime
	 *            空闲资源最大存活时间
	 * @param keepAliveTimeUnit
	 *            空闲资源最大存活时间单位
	 * @param step
	 *            资源递增的步长
	 * @param checkOutDurativeTime
	 *            获取资源时等待的时间
	 * @param checkOutDurativeTimeUnit
	 *            获取资源时等待的时间单位
	 */
	public ThreadPoolConfig(int minPoolSize, int maxPoolSize, int maxIdlePoolSize,
			long keepAliveTime, int step, long checkOutDurativeTime){
		this.minPoolSize     = minPoolSize;
		this.maxPoolSize     = maxPoolSize;
		this.maxIdlePoolSize = maxIdlePoolSize;
			
		this.keepAliveTime   = keepAliveTime;
		this.step            = step;
		this.checkOutDurativeTime = checkOutDurativeTime;
	}

	/**
	 * 获取资源池最小值.<br>
	 * @return 资源池最小值
	 */
	public int getMinPoolSize() {
		return minPoolSize;
	}

	/**
	 * 设置资源池最小值.<br>
	 * @param minPoolSize
	 *            资源池最小值
	 */
	public void setMinPoolSize(int minPoolSize) {
		if( minPoolSize <0 )
			this.minPoolSize = 0;
		else
			this.minPoolSize = minPoolSize;
	}

	/**
	 * 获取资源池最大值.<br>
	 * @return 资源池最小值
	 */
	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	/**
	 * 设置资源池最大值.<br>
	 * @param maxPoolSize
	 *            资源池最小值
	 */
	public void setMaxPoolSize(int maxPoolSize) {
		if( maxPoolSize < 0 )
			this.maxPoolSize = MAX_POOL_SIZE_DEFAULT;
		else
			this.maxPoolSize = maxPoolSize;
	}

	/**
	 * 获取允许最大空闲资源大小.<br>
	 * @return 允许最大空闲资源大小
	 */
	public int getMaxIdlePoolSize() {
		return maxIdlePoolSize;
	}

	/**
	 * 设置允许最大空闲资源大小.<br>
	 * @param maxIdlePoolSize
	 *            允许最大空闲资源大小
	 */
	public void setMaxIdlePoolSize(int maxIdlePoolSize) {
		this.maxIdlePoolSize = maxIdlePoolSize;
	}

	/**
	 * 获取空闲资源最大存活时间.<br>
	 * @return 空闲资源最大存活时间
	 */
	public long getKeepAliveTime() {
		return keepAliveTime;
	}

	/**
	 * 设置空闲资源最大存活时间.<br>
	 * @param keepAliveTime
	 *            空闲资源最大存活时间
	 * @param unit
	 *            时间单位
	 */
	public void setKeepAliveTime(long keepAliveTime) {
		
		this.keepAliveTime = keepAliveTime;
	}

	/**
	 * 获取资源池中资源递增的步长.<br>
	 * @return 资源池中资源递增的步长
	 */
	public int getStep() {
		return step;
	}

	/**
	 * 设置资源池中资源递增的步长.<br>
	 * @param step 资源池中资源递增的步长
	 */
	public void setStep(int step) {
		this.step = step;
	}

	/**
	 * 获取检出资源时等待的时间.<br>
	 * @return 检出资源时等待的时间
	 */
	public long getCheckOutDurativeTime() {
		return checkOutDurativeTime;
	}

	/**
	 * 设置检出资源时等待的时间.<br>
	 * @param checkOutDurativeTime
	 *            检出资源时等待的时间
	 */
	public void setCheckOutDurativeTime(long checkOutDurativeTime) {
		this.checkOutDurativeTime = checkOutDurativeTime;
	}

}
