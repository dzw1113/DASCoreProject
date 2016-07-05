package com.icip.das.core.validate;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.data.TableConfigBean.MergeMappingConfig;
import com.icip.das.core.exception.SysException;
import com.icip.das.core.jdbc.QueryMergeConfig;
import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月8日 下午5:10:05 
 * @update	
 */
@SuppressWarnings("unchecked")
public class DataMergeFilter implements IDataFilter<WorkerProxyObject>{

	@Override
	public WorkerProxyObject handle(WorkerProxyObject proxy) throws SysException {
		List<MergeMappingConfig> configList = QueryMergeConfig.getConfig((TableConfigBean) proxy.getConfig());
		
		if(null == configList || configList.isEmpty())
			return proxy;
		
		List<? extends Map<String,String>> dataList = (List<? extends Map<String, String>>) proxy.getData();
		for(int i = 0; i < dataList.size(); i++){
			Map<String,String> data = dataList.get(i);
			for(int j = 0; j < configList.size(); j++){
				doMergeHandle(data,configList.get(j));
			}
		}
		return proxy;
	}

	/**
	 * @Description: 合并多个字段,删除原字段
	 * @param @param data
	 * @param @param mergeMappingConfig   
	 * @return void  
	 * @throws
	 * @author
	*/
	private void doMergeHandle(Map<String, String> data,
			MergeMappingConfig config) {
		String[] keys = config.getKeyList().split("\\|");// "|"转义
		StringBuilder newValue = new StringBuilder();
		for(int i = 0; i < keys.length; i++){
			//空值不拼接,只删除原字段
			String temp = data.get(keys[i]);
			if(!StringUtils.isBlank(temp)){
				newValue.append(temp);
			}
			data.remove(keys[i]);
		}
		data.put(config.getMergeKey(), newValue.toString());
	}
	
}
