package com.icip.das.core.validate;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.data.TableConfigBean.FieldMappingConfig;
import com.icip.das.core.exception.SysException;
import com.icip.das.core.jdbc.QueryFieldMappingConfig;
import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年2月26日 下午2:57:49 
 * @update	
 */
@SuppressWarnings("unchecked")
public class DataMappingFilter implements IDataFilter<WorkerProxyObject>{

	public WorkerProxyObject handle(WorkerProxyObject proxy)throws SysException{
		List<FieldMappingConfig> configList = QueryFieldMappingConfig.getConfig((TableConfigBean) proxy.getConfig());
		if(null == configList || configList.isEmpty())
			return proxy;

		List<? extends Map<String,String>> dataList = (List<? extends Map<String, String>>) proxy.getData();
		for(int i = 0; i < dataList.size(); i++){
			Map<String,String> data = dataList.get(i);
			for(int j = 0; j < configList.size(); j++){
				doReplace(data,configList.get(j));
			}
		}
		
		return proxy;
	}

	/**
	 * @Description: TODO
	 * @param @param data
	 * @param @param fieldMappingConfig   
	 * @return void  
	 * @throws
	 * @author
	*/
	private void doReplace(Map<String, String> data,
			FieldMappingConfig config) {
		String regexKey = config.getRegexKey();
		if(!StringUtils.isBlank(regexKey) && data.containsKey(regexKey)){
			data.put(config.getTargetKey(), data.get(regexKey));
			data.remove(regexKey);
		}
		String regexValue = config.getRegexValue();
		if(!StringUtils.isBlank(regexValue) && data.containsKey(config.getTargetKey())){//&&之后必须用key值判断
			data.put(config.getTargetKey(), config.getTargetValue());
		}
	}
	
}
