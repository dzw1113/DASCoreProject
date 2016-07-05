package com.icip.framework.topology.redis;

import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.alibaba.fastjson.JSONObject;

public class QueryDataBySTR extends BaseFunction {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String str = tuple.getString(0);
		HashMap<String,String> map = new HashMap<String,String>();
		try {
			map = JSONObject.parseObject(str, HashMap.class);
			String year = map.get("billDate").toString().substring(0,4);
			String month = map.get("billDate").toString().substring(4,6);
			String roomNo = map.get("areaNo")+map.get("unitNo")+map.get("roomNo");
			String paymentAmount = StringUtils.isEmpty(map.get("paymentAmount"))?"0":map.get("paymentAmount");
			String receivedAmount = StringUtils.isEmpty(map.get("receivedAmount"))?"0":map.get("receivedAmount");
			Float arrears = Float.parseFloat(paymentAmount) - Float.parseFloat(receivedAmount);
			String chargeDuring = map.get("startDate") + "-" + map.get("endDate");
			map.put("year", year);
			map.put("month", month);
			map.put("roomNo", roomNo);
			map.put("arrears", arrears.toString());
			map.put("chargeDuring", chargeDuring);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(map.size()>0){
			collector.emit(new Values(map));
		}
	}
	
}
