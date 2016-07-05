package com.icip.framework.function.data;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONObject;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class QueryFieldsBySTRFun extends BaseFunction {

	private String[] fieldKey;

	public QueryFieldsBySTRFun(String... fieldKey) {
		this.fieldKey = fieldKey;
	}

	private static final long serialVersionUID = -1455995763245459942L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String str = tuple.getString(0);
		Map<String, String> map = JSONObject.parseObject(str, Map.class);
		String newKey = "";
		for (int i = 0; i < fieldKey.length; i++) {
			if (i == 0) {
				newKey = map.get(fieldKey[i]);
			} else {
				newKey = newKey + ":" + map.get(fieldKey[i]);
			}
		}
		if (StringUtils.isBlank(newKey))
			newKey = "";
		collector.emit(new Values(newKey));
	}

}
