package com.icip.das.core.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.DataServer;

/** 
 * @Description: 
 * @author  yzk
 * @date 2016年4月14日 下午6:13:31 
 * @update	
 */
//FIXME requestQueue的判断超时应该改下或者去掉
public class ShutdownThread extends Thread{

	private static final Logger logger = LoggerFactory.getLogger(DataWorker.class);

	public void run(){
		logger.info("-----------执行ShutdownThread----------->");
		RollingMonitorTread.stop = true;
		DataServer.SERVER_INSTANCE.releaseTemp();//停轮询检查线程,等没结束的资源释放	
	}
	
}
