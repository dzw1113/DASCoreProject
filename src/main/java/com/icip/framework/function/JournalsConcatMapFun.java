package com.icip.framework.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月11日 下午12:01:33 
 * @update	
 */
public class JournalsConcatMapFun extends BaseFunction {

	private static final long serialVersionUID = -3605031882483012863L;

	int ct = 0;
	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> businessJournalMap = (Map<String, String>) tuple.get(0);//businessJournals
		Map<String, String> transJournalMap = (Map<String, String>) tuple.get(1);//transJournals
		Map<String,String> resultMap = new HashMap<String,String>();
		
		String transTime = transJournalMap.get("transTime");
		if(!StringUtils.isBlank(transTime)){
			resultMap.put("year", transJournalMap.get("transTime").substring(0, 4));
			resultMap.put("month", transJournalMap.get("transTime").substring(4, 6));
		}
		resultMap.put("cid",transJournalMap.get("cid"));
		resultMap.put("hid",businessJournalMap.get("hid"));
		resultMap.put("chargeNo",businessJournalMap.get("chargeNo"));
		resultMap.put("roomNo",businessJournalMap.get("roomNo"));
		resultMap.put("pname",businessJournalMap.get("pname"));
		
		resultMap.put("businessAmount",businessJournalMap.get("businessAmount"));
		

		collector.emit(new Values(resultMap));
	}
  
}
