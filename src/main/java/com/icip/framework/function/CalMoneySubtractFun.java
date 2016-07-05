package com.icip.framework.function;

import org.apache.commons.lang.StringUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/** 
 * @Description: 计算冲销后尚欠金额
 * @author  
 * @date 2016年4月12日 下午5:08:48 
 * @update	
 */
public class CalMoneySubtractFun extends BaseFunction {

	private static final long serialVersionUID = -7466890222992767774L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String arrearsStr =  tuple.getString(0);
		String offsetAmountStr =  tuple.getString(1);
		float arrearsMoeny = Float.valueOf("0");
		float offsetAmount = Float.valueOf("0");
		if(!StringUtils.isBlank(arrearsStr)){
			arrearsMoeny = Float.valueOf(arrearsStr);
		}
		if(!StringUtils.isBlank(offsetAmountStr)){
			offsetAmount = Float.valueOf(offsetAmountStr);
		}
		if(0 == arrearsMoeny){
			collector.emit(new Values("0"));
			return;
		}
		Float subMoney = arrearsMoeny - offsetAmount;
		collector.emit(new Values(subMoney.toString()));
	}

}
