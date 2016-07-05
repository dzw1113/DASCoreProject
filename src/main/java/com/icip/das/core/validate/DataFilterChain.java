package com.icip.das.core.validate;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.exception.SysException;
import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年2月29日 下午7:23:15 
 * @update	
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class DataFilterChain {
	
	private static final Logger logger = LoggerFactory.getLogger(DataFilterChain.class);

	public static final DataFilterChain CHAIN = new DataFilterChain();

	private List<IDataFilter> handlerList;
	
	public DataFilterChain(){
		this.handlerList = new ArrayList<IDataFilter>();
		handlerList.add(new DataBlankFilter());
		handlerList.add(new DataLogicFilter());
		handlerList.add(new DataMergeFilter());
		handlerList.add(new DataCustomerFilter());
		handlerList.add(new DataMappingFilter());
	}
	
	public void addHandler(IDataFilter handler){
		this.handlerList.add(handler);
	}
	 
	public void destroy(){
		this.handlerList.clear();
	}
	 
	public void doHandle(WorkerProxyObject proxy){
		try{
			IDataFilter handler = null;
			for(int i = 0; i < this.handlerList.size(); i++){
				handler = this.handlerList.get(i);
				handler.handle(proxy);
			}
		}catch(SysException e){
			logger.error(e.toString(),e);
			throw e;
		}catch(Exception ex){
			logger.error(ex.toString(),ex);
			throw ex;
		}
	}
	 
}
