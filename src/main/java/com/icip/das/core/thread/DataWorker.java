package com.icip.das.core.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.IResourceObject;
import com.icip.das.core.exception.SysException;
import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;

/**
 * @Description: 
 * @author yzk
 * @date 2016年3月8日 上午9:08:37
 * @update
 */
public class DataWorker extends Thread implements IResourceObject {
	
	private static final Logger logger = LoggerFactory.getLogger(DataWorker.class);

	private static final int STOPPED = 0;
	private static final int STARTING = 1;
	private static final int STARTED = 2;
	private static final int STOPPING = 3;

	/** 请求对象 */
	private volatile WorkerPoolManager.WorkerProxyObject requestProxyObject;

	/** 数据处理器 */
	private IDataHandler handler;

	/** 线程所在的资源池 */
	private WorkerPoolManager workerManager;

	/** 该线程运行状态 */
	private volatile int status;

	/**
	 * 构造器
	 * @param handler
	 */
	public DataWorker(IDataHandler handler) {
		this.status = STOPPED;
		this.requestProxyObject = null;
		this.handler = handler;
	}

	/**
	 * 实现 IResourceObject 的接口方法 － 初始化资源；
	 */
	public void initilize() {
		this.handler = new MainDataHandler(); 
		startService();
	}

	/**
	 * 实现线程的运行方法；
	 */
	public void run() {
		status = STARTED;
		while (status == STARTED){
			Object result = null;
			try {
				this.waitForRequest();
				this.readForRequest();
				this.handleRequest(this.requestProxyObject);
//				this.requestProxyObject.setResult(result);非异常时不设置了
			} catch (Throwable ex) {
				logger.error("DataWorker 捕获了异常--------------");
				logger.error(ex.getMessage(),ex);
				result = ex;
				this.requestProxyObject.setResult(result);
				//TODO
			} finally {
				this.requestDoneTemp();
			}
		}
		status = STOPPED;
	}

	/**
	 * 实现 IResourceObject 的接口方法 － 释放资源 ；
	 */
	public void release() {
		stopService();
	}

	/**
	 * 线程等待请求方法
	 */
	private synchronized void waitForRequest() throws SysException {
		while (this.requestProxyObject == null) {
			// stop中断线程等待
			if (status == STOPPING || status == STOPPED)
				throw new SysException(SysException.SYSTEM_ERROR,"interrupt thread");
			// 等待请求
			try {
				wait(getWaitingTimeout());
			} catch (InterruptedException _ex) {
				// 被异常中断的线程，需要进行丢弃处理
				// this.workerManager.discardRunningWorker(this);
			}
		}
	}

	/**
	 * 为下一个数据做好了准备
	 */
	private void readForRequest() {
		this.handler.getDataResource(this.requestProxyObject);
	}

	/**
	 * 有数据到来
	 * @param obj 数据对象
	 */
	public void onRequest(WorkerPoolManager.WorkerProxyObject obj) {
		if (obj == null)
			return;

		this.requestProxyObject = obj;
		synchronized (this) {
			notify();
		}
	}

	/**
	 * 处理请求对象
	 */
	private void handleRequest(WorkerPoolManager.WorkerProxyObject proxy) {
		this.handler.handleData(this.requestProxyObject);
	}

	/**
	 * 一次服务结束后，把自己释放到空闲池中
	 */
	private void requestDoneTemp() {
		WorkerProxyObject proxy = this.requestProxyObject;
		if((null != this.requestProxyObject) && !(proxy.getResult() instanceof Throwable)){
			this.handler.doneWork(this.requestProxyObject);
		}
		requestProxyObject = null;
		this.workerManager.releaseBusyWorker(this);
	}
	
	/**
	 * 一次服务结束后，把自己释放到空闲池中
	 */
	private void requestDone() {
		WorkerProxyObject proxy = this.requestProxyObject;
		if(!(proxy.getResult() instanceof Throwable)){
			this.handler.doneWork(this.requestProxyObject);
		}
//		synchronized (requestProxyObject) {
//			// 通知等待者，已经处理完成.
//			requestProxyObject.notifyAll();
//		}
		requestProxyObject = null;
		this.workerManager.releaseBusyWorker(this);
	}

	/**
	 * 取请求对象
	 * @return 请求对象
	 */
	public Object getRequestObject() {
		return (this.requestProxyObject);
	}

	/**
	 * 取等待超时时间　
	 * @return 超时时间
	 */
	public long getWaitingTimeout() {
		return (this.workerManager.getPoolConfig().getServiceTimeout());
	}

	/**
	 * 设置服务运行状态；
	 * @param b 状态
	 */
	public void setRunningFlag(boolean b) {
		if (b)
			this.status = STARTED;
		else
			this.status = STOPPED;
	}

	/**
	 * 设置服务运行状态；
	 * @return b 状态
	 */
	public boolean getRunningFlag() {
		if (this.status == STARTED)
			return true;
		else
			return false;
	}

	/**
	 * 克隆对象，实现资源对象的复制
	 */
	public Object clone() {
		DataWorker worker = null;
		IDataHandler hdler = null;

		hdler = (IDataHandler) this.handler.clone();
		worker = new DataWorker(hdler);
		worker.setWorkerManager(this.workerManager);
		return (worker);
	}

	/**
	 * 开始数据服务
	 */
	public void startService() {
		while (status != STOPPED) {
			try {
				//TODO
			} catch (Exception e) {
				// log.error(e);
			}
		}

		status = STARTING;
		start();
	}

	/**
	 * 停止数据服务；
	 */
	public void stopService() {
		status = STOPPING;
		synchronized (this) {
			notify();
		}
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public IDataHandler getHandler() {
		return handler;
	}

	public void setHandler(IDataHandler handler) {
		this.handler = handler;
	}

	public WorkerPoolManager getWorkerManager() {
		return workerManager;
	}

	public void setWorkerManager(WorkerPoolManager workerManager) {
		this.workerManager = workerManager;
	}

}
