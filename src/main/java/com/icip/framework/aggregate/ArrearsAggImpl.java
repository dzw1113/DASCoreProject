package com.icip.framework.aggregate;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/** 
 * @Description: 尚欠款
 * @author  
 * @date 2016年3月30日 上午9:09:56 
 * @update	
 */
public class ArrearsAggImpl implements CombinerAggregator<Map<String,Float>> {

	private static final long serialVersionUID = -4068600207321529785L;

	private String[] fieldKeys;
	
	public ArrearsAggImpl(String... fieldKeys) {
		this.fieldKeys = fieldKeys;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String,Float> init(TridentTuple tuple) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		
		Map<String,Float> re = new HashMap<String,Float>();
		Float paymentAmount = Float.parseFloat(StringUtils.isEmpty(map
				.get(fieldKeys[0])) ? "0" : map.get(fieldKeys[0]));
		
		Float receivedAmount = Float.parseFloat(StringUtils.isEmpty(map
				.get(fieldKeys[1])) ? "0" : map.get(fieldKeys[1]));
		
		Float arrears = paymentAmount - receivedAmount;
		re.put("paymentAmount", paymentAmount);
		re.put("receivedAmount", receivedAmount);
		re.put("arrears", arrears);
		return re;
	}

	@Override
	public Map<String,Float> combine(Map<String,Float> val1, 
			Map<String,Float> val2) {
		Float val1Payment = val1.get("paymentAmount");
		Float val2Payment = val2.get("paymentAmount");
		Float val1recement = val1.get("receivedAmount");
		Float val2recement = val2.get("receivedAmount");

		if(null == val1Payment){
			val1Payment = Float.parseFloat("0");
		}
		if(null == val2Payment){
			val2Payment = Float.parseFloat("0");
		}
		if(null == val1recement){
			val1recement = Float.parseFloat("0");
		}
		if(null == val2recement){
			val2recement = Float.parseFloat("0");
		}
		
		Float payableTotalAmount = val1Payment + val2Payment;
		Float receivedTotalAmount = val1recement + val2recement;
		Float arrears = payableTotalAmount - receivedTotalAmount;
		Map<String,Float> re = new HashMap<String,Float>();
		re.put("payableTotalAmount", payableTotalAmount);
		re.put("receivedTotalAmount", receivedTotalAmount);
		re.put("arrears", arrears);
		return re;
	}

	@Override
	public Map<String,Float> zero() {
		Map<String,Float> re = new HashMap<String,Float>();
		re.put("paymentAmount",Float.parseFloat("0"));
		re.put("receivedAmount", Float.parseFloat("0"));
		re.put("arrears", Float.parseFloat("0"));
		return re;
	}

}
