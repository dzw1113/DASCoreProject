package com.icip.das.core.thread;

/** 
 * @Description: 工作线程池（工人）配置
 * @author  yzk
 * @date 2016年3月7日 下午9:13:37 
 * @update	
 */
public class WorkerPoolConfig implements java.io.Serializable{

	private static final long serialVersionUID = -5352724576038866636L;

	/** 缺省最大服务排队数        */
	private final int             DEFAULT_MAX_QUEUE_SIZE         = 100;
	
	/** 缺省工作服务超时时间：6小时   */
	private final int             DEFAULT_SERVICE_TIMEOUT        = 6 * 1000 * 3600;

	/**缺省最小服务排队数**/
	private final int 			  DEFAULT_MIN_QUEUE_SIZE         = 10;
	
	/** 最大请求排队队列数        */
	private int                   maxQueueSize;
	
	/** 服务超时时间   */
	private int                   serviceTimeout;

	/** 资源池 */
	ThreadPoolConfig 			  resourcePoolConfig ;
	
	public WorkerPoolConfig() {
		super();
		this.maxQueueSize   = DEFAULT_MAX_QUEUE_SIZE;
		this.serviceTimeout = DEFAULT_SERVICE_TIMEOUT;
		this.resourcePoolConfig = new ThreadPoolConfig() ; 
	}
	
	/**
	 * @param type 参数可选1,2,3对应低，中，高配置
	 */
	public WorkerPoolConfig(int type){
		super();
		switch(type){
			case 1 :
				this.maxQueueSize = DEFAULT_MIN_QUEUE_SIZE;
				this.serviceTimeout = DEFAULT_SERVICE_TIMEOUT;
				this.resourcePoolConfig = new ThreadPoolConfig(10,10);
				break;
			case 2 :
				this.maxQueueSize = DEFAULT_MIN_QUEUE_SIZE;
				this.serviceTimeout = DEFAULT_SERVICE_TIMEOUT;
				this.resourcePoolConfig = new ThreadPoolConfig(20,40);
				break;
			case 3 :
				this.maxQueueSize   = DEFAULT_MAX_QUEUE_SIZE;
				this.serviceTimeout = DEFAULT_SERVICE_TIMEOUT;
				this.resourcePoolConfig = new ThreadPoolConfig() ; 
				break;
		}
	}
	
	public int getMaxQueueSize() {
		return maxQueueSize;
	}

	public void setMaxQueueSize(int maxQueueSize) {
		this.maxQueueSize = maxQueueSize;
	}

	public int getServiceTimeout() {
		return serviceTimeout;
	}

	public void setServiceTimeout(int serviceTimeout) {
		this.serviceTimeout = serviceTimeout;
	}

	public ThreadPoolConfig getResourcePoolConfig() {
		return resourcePoolConfig;
	}

	public void setResourcePoolConfig(ThreadPoolConfig resourcePoolConfig) {
		this.resourcePoolConfig = resourcePoolConfig;
	}
	
}
