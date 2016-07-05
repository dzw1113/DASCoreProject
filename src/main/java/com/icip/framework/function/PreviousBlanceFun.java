package com.icip.framework.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/** 
 * @Description: 计算本期上期余额 预缴表余额+抵充-本月预缴金额
 * @author  
 * @date 2016年3月30日 下午4:21:28 
 * @update	
 */
public class PreviousBlanceFun extends BaseFunction{

	private static final long serialVersionUID = -322622863251623117L;
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String currentAmountStr = tuple.getString(0);
		String prepaidAmountStr =  tuple.getString(1);
		String offsetAmountStr = tuple.getString(2);
		Float currentAmount = Float.valueOf(currentAmountStr);
		if(currentAmount.equals(Float.valueOf("0"))){//预缴表余额0,返回
			collector.emit(new Values("0"));
			return;
		}
		
		Float prepaidAmount = Float.parseFloat(prepaidAmountStr);
		//本月无预缴,返回预缴表余额
		if(prepaidAmount.equals(Float.valueOf("0"))){
			collector.emit(new Values(currentAmount.toString()));
			System.out.println("本月无预缴,返回预缴表余额" + currentAmount);
			return;
		}
		
		//本月有预缴,余额=预缴表余额+抵充-本月预缴
		Float offsetAmount = Float.parseFloat(offsetAmountStr);
		Float temp = currentAmount + offsetAmount - prepaidAmount;
		collector.emit(new Values(temp.toString()));
		
	}

}
