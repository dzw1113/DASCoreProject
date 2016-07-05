package com.icip.das.core.validate.handler;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;
import com.icip.das.core.validate.ICustomerHandler;

/** 
 * @Description: 查询应交款处理类
 * @author  
 * @date 2016年3月24日 下午4:15:44 
 * @update	
 */
@SuppressWarnings("unchecked")
public class ArrearsStatisticHandler implements ICustomerHandler<WorkerProxyObject>{

	@Override
	public WorkerProxyObject handle(WorkerProxyObject proxy) {
		List<? extends Map<String,String>> params = (List<? extends Map<String, String>>) proxy.getData();
		for(Map<String,String> param : params){
			String billData = param.get("billDate");
			if(!StringUtils.isBlank(billData)){
				String year = billData.substring(0,4);
				String month = billData.substring(4,6);
				param.put("year", year);
				param.put("month", month);
//				param.remove("billData");保留原字段
			}
			
			String startDate = param.get("startDate");
			String endDate = param.get("endDate");
			param.put("chargeDuring", startDate + "-" + endDate);//收费期间
		}
		return proxy;
	}

}
