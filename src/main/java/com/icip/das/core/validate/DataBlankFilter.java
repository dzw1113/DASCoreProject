package com.icip.das.core.validate;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.data.TableConfigBean.BlankMappingConfig;
import com.icip.das.core.exception.SysException;
import com.icip.das.core.jdbc.QueryBlankMappingConfig;
import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年2月29日 下午8:25:46 
 * @update	
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class DataBlankFilter implements IDataFilter<WorkerProxyObject>{
	
	private static final String BLANK = "";

	//如果没有配置空值映射所有空字段会被映射为空字符串
	@Override
	public WorkerProxyObject handle(WorkerProxyObject proxy) throws SysException {
		List<BlankMappingConfig> configList = QueryBlankMappingConfig.getConfig((TableConfigBean) proxy.getConfig());
		List<? extends Map<String,String>> dataList = (List<? extends Map<String, String>>) proxy.getData();
		if(null == configList || configList.isEmpty()){
			blankMapping(dataList);
			return proxy;
		}
		
		for(int i = 0; i < dataList.size(); i++){
			Map<String,String> data = dataList.get(i);
			for(int j = 0; j < configList.size(); j++){
				String column = configList.get(j).getKey();
				String value = data.get(column);
				if(StringUtils.isBlank(value)){
					data.put(column, configList.get(j).getBlankValue());
				}
			}
		}
		blankMapping(dataList);//没有配置的字段如果出现空值替换为默认的
		return proxy;
	}

	private void blankMapping(List<? extends Map<String,String>> dataList){
		for(Map<String,String> param : dataList){
			Set entries = param.entrySet();  
			Iterator iterator = entries.iterator();  
			while(iterator.hasNext()) {
			   Map.Entry<String,String> entry = (Entry) iterator.next();
			   String value = entry.getValue();
			   if(StringUtils.isBlank(value)){
				   String key = entry.getKey();  
				   param.put(key, BLANK);
			   }
			}  
		}
	}
	
}
