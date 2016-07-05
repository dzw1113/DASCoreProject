package com.icip.framework.function.common;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 从Data中取数据，指定多个key时以英文:分隔
 * @author dzw
 * @date 2016年3月10日 下午5:35:52
 * @update
 */
public class QueryFieldsByDataFun extends BaseFunction {

	private String[] fieldKey;

	public QueryFieldsByDataFun(String... fieldKey) {
		this.fieldKey = fieldKey;
	}

	private static final long serialVersionUID = -1455995763245459942L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		String newKey = "";
		for (int i = 0; i < fieldKey.length; i++) {
			if (i == 0) {
				newKey = map.get(fieldKey[i]);
			} else {
				newKey = newKey + ":" + map.get(fieldKey[i]);
			}
		}
		if(StringUtils.isBlank(newKey))
			newKey = "";
		collector.emit(new Values(newKey));
	}

}
