package com.icip.das.core;

import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.exception.SysException;
import com.icip.das.core.thread.MainDataHandler;
import com.icip.das.core.thread.WorkerPoolManager;

/**
 * @Description: 数据处理服务器
 * @author yzk
 * @date 2016年3月8日 上午10:11:25
 * @update
 */
public class DataServer {

	private static final Logger logger = LoggerFactory.getLogger(DataServer.class);
	
	public static volatile boolean IS_RUNNING = false;

	public static final DataServer SERVER_INSTANCE = new DataServer();

	/** 服务工人 */
	private MainDataHandler worker = new MainDataHandler();// 暂时做死了

	/** 数据服务器资源管理 (资源池) */
	private WorkerPoolManager workerManager;

	public void startServer() {
		IS_RUNNING = true;
		logger.debug("---------DataServer 数据服务器启动--------->");
		SERVER_INSTANCE.initilize();
	}

	/**
	 * 初始化数据服务器资源
	 */
	private void initilize() {
		workerManager = new WorkerPoolManager(worker);
		workerManager.initlize();// 缺省配置
		this.workerManager.start();
	}

	/**
	 * 停止数据服务器
	 */
	public void stopServer() {
		this.workerManager.stop();
	}

	/**
	 * 释放数据服务器资源
	 */
	public void releaseTemp() {
		this.workerManager.releaseTemp();
	}
	
	/**
	 * 释放数据服务器资源
	 */
	public void release() {
		this.workerManager.release();
		this.workerManager = null; 
	}

	/**
	 * 处理数据
	 * 
	 * @param request
	 *            - 处理数据
	 * @param timeout
	 *            - 处理超时时间
	 * @param handleTime
	 *            - 处理时间
	 * @return
	 * @throws TimeoutException 
	 */
	public Object handleData(TableConfigBean config, long timeout,String sql,boolean isFinish) throws Exception {
		Object dataResult = null;
		try {
			Object res = this.workerManager.syncHandle(config, timeout,System.currentTimeMillis(),sql,isFinish);//FIXME 处理时间做死了
			if (res instanceof SysException) {
				logger.error("数据处理异常", (SysException) res);
			} else {
				dataResult = res;
			}
		} catch (TimeoutException te) {
			logger.error("数据处理超时" + config.toString(), te);
			throw te;
		} catch (SysException syse) {
			if (SysException.SYSTEM_LIMITED.equals(syse.getExceptionCode())) {
				// TODO 失败是由于队列拒绝加入
			}
			logger.error("数据处理异常" + config.toString(), syse);
			throw syse;
		} catch (Exception e) {
			logger.error("数据处理异常，系统异常" + config.toString(), e);
			throw e;
		}
		return dataResult;
	}

}
