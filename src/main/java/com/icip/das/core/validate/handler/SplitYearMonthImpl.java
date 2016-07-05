package com.icip.das.core.validate.handler;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;
import com.icip.das.core.validate.ICustomerHandler;

/** 
 * @Description: 截取billDate中年，月
 * @author  
 * @date 2016年3月18日 下午2:24:39 
 * @update	
 */
@SuppressWarnings("unchecked")
public class SplitYearMonthImpl implements ICustomerHandler<WorkerProxyObject>{

	//billData格式20151124
	@Override
	public WorkerProxyObject handle(WorkerProxyObject proxy) {
		List<? extends Map<String,String>> params = (List<? extends Map<String, String>>) proxy.getData();
		for(Map<String,String> param : params){
			String billDate = param.get("billDate");
			if(!StringUtils.isBlank(billDate)){
				String year = billDate.substring(0,4);
				String month = billDate.substring(4,6);
				param.put("year", year);
				param.put("month", month);
//				param.remove("billData");保留原字段
			}
		}
		return proxy;
	}

}
