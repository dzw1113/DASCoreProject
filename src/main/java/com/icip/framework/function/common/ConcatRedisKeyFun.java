package com.icip.framework.function.common;

import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 拼接redis查询key
 * @author dzw
 * @date 2016年3月10日 下午3:31:32
 * @update
 */
public class ConcatRedisKeyFun extends BaseFunction {

	private String tbKey;
	private String[] fieldKey;

	public ConcatRedisKeyFun(String tbKey, String... fieldKey) {
		this.tbKey = tbKey;
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
				newKey = newKey
						+ ":"
						+ (fieldKey[i].equals("*") ? "*" : map.get(fieldKey[i]));
			}
		}
		collector.emit(new Values(tbKey + ":" + newKey));
	}

}
