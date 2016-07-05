package com.icip.das.core.thread;

import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.exception.SysException;

/** 
 * @Description: 服务调度管理器，为请求分配服务资源，回收处理完成的工作线程
 * @author  yzk
 * @date 2016年3月7日 下午9:15:31 
 * @update	
 */
public class WorkerPoolManager {
	
	private static final Logger logger = LoggerFactory.getLogger(WorkerPoolManager.class);

	/** 运行状态：停止     */
	public final static short     WORK_STATUS_STOPPED     = 0;
	
	/** 运行状态：运行     */
	public final static short     WORK_STATUS_RUNNING     = 1;
	
	private final ReentrantLock   mainLock    = new ReentrantLock();

	/** 资源池参数      */
	private WorkerPoolConfig                   poolConfig; 

	/** 请求队列            */
	private BlockingQueue<WorkerProxyObject>   requestQueue;
	
	/** 服务工人池      */
	private ThreadPool                 		   serviceWorkerPool;

	/** 工作处理器      */
	private IDataHandler                       handler;

	/** 工作状态          */
	private short                              workStatus;
	
	private RunningStatus  					   runningStatus;
	
	/**
	 * 构造器
	 * @param handler
	 */
	public WorkerPoolManager(IDataHandler handler) {
		this.handler    = handler;
		this.workStatus = WORK_STATUS_STOPPED;
	}

	/**
	 * 初始化工作资源
	 */
	public void initlize() {
		/** 如果外面不设置，使用缺省配置    */
		if( this.poolConfig == null )
			this.poolConfig = new WorkerPoolConfig();
		
		requestQueue = new LinkedBlockingQueue<WorkerProxyObject>(poolConfig.getMaxQueueSize());
		
		DataWorker worker = new DataWorker(this.handler);
		worker.setWorkerManager(this);
		this.serviceWorkerPool = new ThreadPool(worker);
		this.serviceWorkerPool.setConfig(this.poolConfig.getResourcePoolConfig());
		this.serviceWorkerPool.initialize();
		runningStatus = new RunningStatus();
		this.start();
	}
	
	/**
	 * 启动对外服务
	 */
	public void start() {
		this.workStatus = WORK_STATUS_RUNNING;
	}
	
	/**
	 * 停止对外服务
	 */
	public void stop() {
		this.workStatus = WORK_STATUS_STOPPED;
	}
	
	public void releaseTemp() {
		this.serviceWorkerPool.destroyTemp();
	}
	
	/**
	 * 释放工作资源
	 */
	public void release() {
		this.stop();
		this.requestQueue.clear();  //野蛮Clear，估计会有问题
		this.requestQueue = null;
		
		this.handler = null;
		
		this.serviceWorkerPool.destroy();
		this.serviceWorkerPool = null;
	}
	
	/**
	 * 分配请求
	 * @param requestObject
	 * @param waitTimeout
	 */
	private void dispatchRequest(WorkerProxyObject proxyObject)
			throws Exception{
		mainLock.lock();
		try {
			if(proxyObject == null){
				return;
			}
			if(this.requestQueue.size() >= poolConfig.getMaxQueueSize())
			      throw new SysException(SysException.SYSTEM_LIMITED , "请求队列超过最大限制!");

			if(judgeTimeOut(proxyObject)){
				runningStatus.addTimeOutNum();
				throw new InterruptedException(SysException.SYSTEM_TIMEOUT);
			}
			DataWorker worker = (DataWorker)this.serviceWorkerPool.getFreeResource(0); //TODO 取出时间上限
			worker.onRequest(proxyObject);
		} catch(TimeoutException te){
			logger.error(te.getMessage(),te);
			proxyObject.setTimeOut(true);//设置为超时
			this.requestQueue.add(proxyObject);
			throw te;
		}catch(SysException ex){
			logger.error(ex.getMessage(),ex);
			proxyObject.setRejected(true);//设置为被拒绝
			throw new SysException(SysException.SYSTEM_LIMITED , "请求队列超过最大限制!");
		}catch(InterruptedException ex){
			logger.error(ex.getMessage(),ex);
			throw new InterruptedException(SysException.SYSTEM_TIMEOUT);
		}catch (Exception e) {
			logger.error(e.getMessage(),e);
			this.requestQueue.add(proxyObject);
			throw e;
		} finally {
			mainLock.unlock();
		}
	}

	public boolean judgeTimeOut(WorkerProxyObject obj){
    	long requestTime = obj.getRequestTime();//请求时间
    	long nowTime = new Date().getTime();
    	long timeOut = obj.getWaitTimeout();
    	if((nowTime - requestTime)>timeOut){//如果现在时间减去请求时间大于超时时间则不执行该请求 ，直接返回
    		obj.setTimeOut(true);//设置为超时后 人工打断抛出异常，因为处理时间已经超过前台等待时间，无需再处理
    		return true;
    	}else{
    		obj.setTimeOut(false);//原来可能是超时导致的 设置回去
    		return false;
    	}
	}
	
	/**
	 * 等待结果
	 * @param proxyObject
	 * @param waitTimeout
	 * @throws InterruptedException
	 */
	public void waitResult(WorkerProxyObject proxyObject, long waitTimeout) throws InterruptedException {
		synchronized (proxyObject) {
            proxyObject.wait( waitTimeout );
	    }
	}
	
	/**
	 * 释放工作完成的工作线程
	 * @param worker
	 */
	public synchronized void releaseBusyWorker(DataWorker worker) {
        if( this.requestQueue.size() > 0){
        	WorkerProxyObject obj = requestQueue.poll();
        	if(judgeTimeOut(obj)){
        		runningStatus.addTimeOutNum();//如果超时了就释放资源然后返回
        		this.serviceWorkerPool.releaseBusyResource(worker);
        		return ;
        	}
            worker.onRequest(obj);
            return;
        }
        else{
    		this.serviceWorkerPool.releaseBusyResource(worker);
        }
	}
	
	/**
	 * 数据处理
	 * @param data
	 * @param waitTimeout
	 * @return 
	 *       1、Object       - 数据处理结果
	 *       2、SysException - 数据处理异常
	 */
	@SuppressWarnings("finally")
	public Object syncHandle(TableConfigBean config, long waitTimeout,long requestTime,String sql,boolean isFinish)
			throws TimeoutException, SysException{
		WorkerProxyObject proxyObject = new WorkerProxyObject(config,sql,isFinish);
		proxyObject.setRequestTime(requestTime);//设置请求时间
		proxyObject.setResult(null);
		if(waitTimeout <= 0 )
			waitTimeout = this.poolConfig.getServiceTimeout();
		proxyObject.setWaitTimeout(waitTimeout);//设置请求的超时时间
		
		try {			
		    this.dispatchRequest(proxyObject);
		    if(proxyObject.isTimeOut()){
		    	throw new InterruptedException(SysException.SYSTEM_TIMEOUT);
		    }
//		    this.waitResult(proxyObject, waitTimeout);
		} catch (InterruptedException ie) {
			SysException res = new SysException(SysException.SYSTEM_THREAD, "DataWorkerThread 异常中断",  ie );
			throw res;
		} catch(Throwable e) {
			SysException res = new SysException(SysException.SYSTEM_ERROR, " 执行DataWorkerThread 系统异常",  e );
			throw res;
		}
		finally {
		    if( proxyObject.getResult() == null && proxyObject.isTimeOut()) {
		    	throw new SysException(SysException.SYSTEM_TIMEOUT, "WorkerThread Process Timeout, WaitTimeout = " + waitTimeout);
		    }
		    else if(proxyObject.getResult() == null && proxyObject.isRejected()){
		    	throw new SysException( SysException.SYSTEM_LIMITED , "请求队列超过最大限制!" );
		    }
		    else if( proxyObject.getResult() == null && proxyObject.getResult() instanceof Throwable) { 
		    	throw new SysException(SysException.SYSTEM_ERROR, (Throwable)proxyObject.getResult());
	    	}
		    else {
		    	return(proxyObject.getResult());
		    }
		}
	}
	
	public int getMaxQueueSize() {
		return poolConfig.getMaxQueueSize();
	}

	public void setMaxQueueSize(int maxQueueSize) {
		poolConfig.setMaxQueueSize(maxQueueSize);
	}

	public int getQueueSize(){
		return requestQueue.size();
	}

	public RunningStatus getRunningStatus() {
		return runningStatus;
	}

	public IDataHandler getHandler() {
		return handler;
	}

	public void setHandler(IDataHandler handler) {
		this.handler = handler;
	}

	public short getWorkStatus() {
		return workStatus;
	}

	public void setWorkStatus(short workStatus) {
		this.workStatus = workStatus;
	}

	public class RunningStatus {
		private AtomicLong timeOutNum = new AtomicLong();
		
		public RunningStatus(){
		}

		public long getTimeOutNum(){
			return timeOutNum.longValue();
		}
		
		public void addTimeOutNum(){
			timeOutNum.getAndIncrement();
		}
		
		public short getRunningStatus() {
			return workStatus;
		}

		public int getCurrentIdleResources() {
			return serviceWorkerPool.getCurrentIdleSize();
		}

		public int getCurrentUsingResources() {
			return serviceWorkerPool.getCurrentUsingSize();
		}

		public int getRequestQueueSize() {
			return requestQueue.size();
		}
		
	}
	
	/**
	 * 工作请求和应答的代理
	 */
	public class WorkerProxyObject {
		
		private Object           config;	

		private Object           result;
		
		private Object			 data;
		
		private long 			 waitTimeout;//请求超时时间
		
		private long 			 requestTime;//请求发起时间，使用(new Date()).getTime();
		
		private boolean			 isTimeOut;//是否超时
		
		private boolean 		 isRejected;//是否被系统拒绝
		
		private String 			 excuteSql;//执行的sql
		
		private boolean isFinish = false;
		// 可以加上请求的开始时间，用于记录在队列中超时的情况下，自动清理。
		
		private boolean isRelease = false;//没查到数据时释放线程标识
		
		public boolean isRelease() {
			return isRelease;
		}

		public void setRelease(boolean isRelease) {
			this.isRelease = isRelease;
		}
		
		public boolean isFinish() {
			return isFinish;
		}

		public void setFinish(boolean isFinish) {
			this.isFinish = isFinish;
		}

		public String getExcuteSql() {
			return excuteSql;
		}

		public void setExcuteSql(String excuteSql) {
			this.excuteSql = excuteSql;
		}

		public boolean isTimeOut() {
			return isTimeOut;
		}

		public void setTimeOut(boolean isTimeOut) {
			this.isTimeOut = isTimeOut;
		}

		public boolean isRejected() {
			return isRejected;
		}

		public void setRejected(boolean isRejected) {
			this.isRejected = isRejected;
		}

		public WorkerProxyObject() {
		}
		
		public Object getConfig() {
			return config;
		}

		public void setConfig(Object config) {
			this.config = config;
		}

		public Object getData() {
			return data;
		}

		public void setData(Object data) {
			this.data = data;
		}
		
		public WorkerProxyObject(Object config,String sql,boolean isFinish) {
			this.config = config;
			this.isTimeOut = false;
			this.isRejected = false;
			this.excuteSql = sql;
			this.isFinish = isFinish;
		}
		
		public long getWaitTimeout() {
			return waitTimeout;
		}

		public void setWaitTimeout(long waitTimeout) {
			this.waitTimeout = waitTimeout;
		}

		public long getRequestTime() {
			return requestTime;
		}

		public void setRequestTime(long requestTime) {
			this.requestTime = requestTime;
		}

		public Object getResult() {
			return result;
		}

		public void setResult(Object result) {
			this.result = result;
		}
	}

	public WorkerPoolConfig getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(WorkerPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

	public ThreadPool getServiceWorkerPool() {
		return serviceWorkerPool;
	}
	
}
