package com.icip.das.core.validate;

import java.lang.reflect.Method;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.exception.SysException;
import com.icip.das.core.jdbc.QueryCustomerHandlerConfig;
import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月8日 下午5:15:39 
 * @update	
 */
public class DataCustomerFilter implements IDataFilter<WorkerProxyObject>{
	
	private static final Logger logger = LoggerFactory.getLogger(DataCustomerFilter.class);

	@Override
	public WorkerProxyObject handle(WorkerProxyObject proxy) throws SysException {
		List<String> handlerConfig = QueryCustomerHandlerConfig.getConfig((TableConfigBean) proxy.getConfig());
		if(null == handlerConfig || handlerConfig.isEmpty()){
			return proxy;
		}
		
		for(int i = 0; i < handlerConfig.size(); i++){
			try{
				Class<?> clazz = Class.forName(handlerConfig.get(i));
				Method splitMethod = clazz.getDeclaredMethod("handle",WorkerProxyObject.class);
				splitMethod.invoke(clazz.newInstance(),proxy);
			}catch(Exception e){
				logger.error(e.getMessage(),e);
			}
		}
		return proxy;
	}

}
