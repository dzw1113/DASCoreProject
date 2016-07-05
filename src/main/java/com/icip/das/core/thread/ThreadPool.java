package com.icip.das.core.thread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.IResourceObject;
import com.icip.das.core.exception.SysException;

/**
 * @Description: TODO
 * @author yzk
 * @date 2016年3月7日 下午8:46:39
 * @update
 */
public class ThreadPool {

	private static final Logger logger = LoggerFactory.getLogger(ThreadPool.class);

	private final ReentrantLock mainLock = new ReentrantLock();

	private final Condition termination = mainLock.newCondition();

	/** 资源池配置 */
	private ThreadPoolConfig config;

	/** 资源实例 */
	private IResourceObject resObject;

	/** 空闲资源池 */
	private List<IResourceObject> idleResources = null;

	/** 工作资源池 */
	private List<IResourceObject> usingResources = null;

	private EvictorTask evictor = null;

	/**
	 * 缺省值资源池
	 * @param obj
	 */
	public ThreadPool(IResourceObject obj) {
		this(new ThreadPoolConfig(), obj);
	}

	public ThreadPool(ThreadPoolConfig config, IResourceObject obj) {
		this(null, config, obj);
	}

	public ThreadPool(String poolName, ThreadPoolConfig config,IResourceObject obj) {
		this.config = config;
		this.resObject = obj;
		initialize();
	}

	public IResourceObject getResObject() {
		return resObject;
	}

	public void setResObject(IResourceObject resObject) {
		this.resObject = resObject;
	}

	public ThreadPoolConfig getConfig() {
		return config;
	}

	public void setConfig(ThreadPoolConfig config) {
		this.config = config;
	}

	/**
	 * 获取允许最大空闲资源大小.<br>
	 * @return
	 */
	public int getMaxIdlePoolSize() {
		return config.getMaxIdlePoolSize();
	}

	/**
	 * 获取资源池最大值.<br>
	 * @return
	 */
	public int getMaxPoolSize() {
		return config.getMaxPoolSize();
	}

	/**
	 * 获取资源池最小值.<br>
	 * @return
	 */
	public int getMinPoolSize() {
		return config.getMinPoolSize();
	}

	/**
	 * 获取资源池中资源递增的步长.<br>
	 * @return
	 */
	public int getStep() {
		return config.getStep();
	}

	/**
	 * 设置允许最大空闲资源大小.<br>
	 * @param maxIdlePoolSize
	 */
	public void setMaxIdlePoolSize(int maxIdlePoolSize) {
		config.setMaxIdlePoolSize(maxIdlePoolSize);
	}

	/**
	 * 设置资源池最大值.<br>
	 * @param maxPoolSize
	 */
	public void setMaxPoolSize(int maxPoolSize) {
		config.setMaxPoolSize(maxPoolSize);
	}

	/**
	 * 设置资源池最小值.<br>
	 * @param mixPoolSize
	 */
	public void setMinPoolSize(int minPoolSize) {
		config.setMinPoolSize(minPoolSize);
	}

	/**
	 * 设置资源池中资源递增的步长.<br>
	 * @param step
	 */
	public void setStep(int step) {
		config.setStep(step);
	}

	/**
	 * 获取空闲资源最大存活时间.<br>
	 * @return
	 */
	public long getKeepAliveTime() {
		return config.getKeepAliveTime();
	}

	/**
	 * 设置空闲资源最大存活时间.<br>
	 * @param keepAliveTime
	 * @param unit
	 */
	public void setKeepAliveTime(long keepAliveTime) {
		config.setKeepAliveTime(keepAliveTime);
	}

	/**
	 * 获取当前空闲资源的大小.<br>
	 * @return
	 */
	public int getCurrentIdleSize() {
		return idleResources.size();
	}

	/**
	 * 获取当前正在使用的资源大小.<br>
	 * @return
	 */
	public int getCurrentUsingSize() {
		return usingResources.size();
	}

	/**
	 * 初始化资源池；
	 * @note: 初始化完成后，池中包含的资源数为： initNum;
	 */
	public void initialize() {
		idleResources = Collections.synchronizedList(new ArrayList<IResourceObject>(config.getMinPoolSize()));
		usingResources = Collections.synchronizedList(new ArrayList<IResourceObject>(config.getMinPoolSize()));

		for (int i = 0; i < config.getMinPoolSize(); i++) {
			if (i + 1 <= config.getMaxIdlePoolSize()) {
				createResource();
			}
		}

		if (config.getKeepAliveTime() > 0) {
			evictor = new EvictorTask();
			CoreTimer.schedule(evictor, config.getKeepAliveTime(),config.getKeepAliveTime());
		}
	}

	private void waitTemp(){
		while(usingResources.size() > 0){
			logger.info("等待资源结束----------->");
			try{
				Thread.sleep(2000);
				waitTemp();
			}catch(InterruptedException ex){
				continue;
			}
		}
		logger.info("工作中的资源全部释放,结束----------->");
	}
	
	public void destroyTemp() {
		if (null != evictor) {
			CoreTimer.cancel(evictor);
		}
		evictor = null;
		if (null != usingResources) {
			waitTemp();
		}
	}
	
	/**
	 * 销毁资源池.<br>
	 */
	public void destroy() {
		if (null != idleResources) {
			for (IResourceObject resource : idleResources) {
				this.discardResourceObject(resource);
			}
			idleResources.clear();
		}
		if (null != usingResources) {
			for (IResourceObject resource : usingResources) {
				this.discardResourceObject(resource);
			}
			usingResources.clear();
		}
		if (null != evictor) {
			CoreTimer.cancel(evictor);
		}
		config = null;
		evictor = null;
	}

	/**
	 * 从资源池中取一个空闲的资源；
	 * @param waitTime  最长等待时间（秒） 如果参数中TimeOut 时间大于零且小于配置，以调用参数为准
	 */
	public IResourceObject getFreeResource(long timeOut)
			throws TimeoutException, SysException {
		long starttime = System.currentTimeMillis();
		long maxWait;
		mainLock.lock();
		try {
			// 如果参数中TimeOut 时间大于零且小于配置，以调用参数为准
			maxWait = config.getCheckOutDurativeTime();
			if (timeOut > 0 && timeOut <= maxWait)
				maxWait = timeOut;

			for (;;) {
				if (idleResources.size() == 0) {// 当空闲的资源为0时，则创建资源，每次创建的数量由config配置中来
					if (usingResources.size() < config.getMaxPoolSize()) {
						for (int i = usingResources.size(); i < usingResources
								.size() + config.getStep(); i++) {
							if (i < config.getMaxPoolSize()) {
								createResource();
							}
						}
					} else {
						try {
							if (maxWait <= 0) {
								termination.await();
							} else {
								final long elapsed = (System.currentTimeMillis() - starttime);
								final long waitTime = maxWait - elapsed;

								if (waitTime > 0) {
									termination.await(waitTime,TimeUnit.MILLISECONDS);
								}
							}
						} catch (InterruptedException e) {
							logger.error(e.toString(), e);
						}
					}
					if (maxWait > 0 && ((System.currentTimeMillis() - starttime) >= maxWait)) {
						throw new SysException(SysException.SYSTEM_TIMEOUT, "等待空闲资源超时" + maxWait + " ms.");
					}
				} else {
					if (maxWait > 0 && ((System.currentTimeMillis() - starttime) >= maxWait)) {
						throw new SysException(SysException.SYSTEM_TIMEOUT, "等待空闲资源超时" + maxWait + " ms.");
					}
					return addToUsingResource();
				}
			}
		} finally {
			mainLock.unlock();
		}
	}

	/**
	 * 把工作结束的资源释放到空闲资源池中；
	 * @param obj 资源对象；
	 */
	public void releaseBusyResource(IResourceObject resObj) {
		mainLock.lock();
		try {
			if (usingResources.remove(resObj)) {
				if ((idleResources.size()) < config.getMaxIdlePoolSize()) {
					addToIdleResource(resObj);
				} else {
					this.discardResourceObject(resObj);
				}
				termination.signalAll();
			}
		} finally {
			mainLock.unlock();
		}
	}

	/**
	 * 丢弃一个工作资源
	 * @param resObj 资源对象；
	 */
	private void discardResourceObject(IResourceObject resObj) {
		resObj.release();
	}

	private synchronized IResourceObject addToUsingResource() {
		mainLock.lock();
		try {
			IResourceObject resource = idleResources.remove(0);
			try {
				usingResources.add(resource);
				return resource;
			} catch (Throwable e) {
				try {
					addToIdleResource(resource);
				} catch (Throwable e2) {
					// ignore
				}
				throw new RuntimeException(this.getClass().getName() + " activate() make error!", e);
			}
		} finally {
			mainLock.unlock();
		}
	}

	private void addToIdleResource(IResourceObject resource) {
		mainLock.lock();
		try {
			idleResources.add(resource);
		} finally {
			mainLock.unlock();
		}
	}

	private void createResource() {
		IResourceObject resource = (IResourceObject) this.resObject.clone();
		mainLock.lock();
		try {
			resource.initilize();
			idleResources.add(resource);
		} finally {
			mainLock.unlock();
		}
	}

	public void evict() {
		mainLock.lock();
		try {
			if (idleResources.isEmpty() || idleResources.size() <= this.getMaxIdlePoolSize()){
				return;
			}
		} finally {
			mainLock.unlock();
		}
		mainLock.lock();
		try {
			// 第0个元素是放入堆栈中最长的元素
			while (idleResources.size() > this.getMaxIdlePoolSize() && idleResources.size() > 0){
				IResourceObject resource = idleResources.remove(0);
				this.discardResourceObject(resource);
			}
		} finally {
			mainLock.unlock();
		}
	}

	/**
	 *	踢除超过最大空闲时间的资源 
	 */
	private class EvictorTask extends TimerTask {
		public void run() {
			try {
				evict();
			} catch (Exception e) {
				// ignored
			}
		}
	}

	/**
	 * 取当前资源池中的资源数
	 * @return
	 */
	public int getCurrentResourceNumber() {
		return (this.idleResources.size() + this.usingResources.size());
	}
	
}
