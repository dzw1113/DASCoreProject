package com.icip.framework.function;

import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 合并map
 * @author
 * @date 2016年4月11日 上午10:08:55
 * @update
 */
public class DataConcatMapFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		Map<String, String> map1 = (Map<String, String>) tuple.get(1);
		Map<String, String> map2 = (Map<String, String>) tuple.get(2);
		map.putAll(map1);
		map.putAll(map2);
		collector.emit(new Values(map));
	}

}
