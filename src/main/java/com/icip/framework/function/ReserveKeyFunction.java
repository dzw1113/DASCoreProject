package com.icip.framework.function;

import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 累计欠费计算key，根据当前月抽取累计欠费
 * @author
 * @date 2016年4月8日 下午2:04:10
 * @update
 */
public class ReserveKeyFunction extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String rdKey = tuple.getString(0);
		Map<String, String> map = (Map<String, String>) tuple.get(1);
		int rdYear = Integer.parseInt(rdKey.split(":")[4]);
		int rdMonth = Integer.parseInt(rdKey.split(":")[5]);

		int dtYear = Integer.parseInt(map.get("year"));
		int dtMonth = Integer.parseInt(map.get("month"));
		// redis中的年小于等于前年小，月小于等于当前月
		if (rdYear <= dtYear && (rdMonth <= dtMonth)) {
			collector.emit(new Values(rdKey));
		}
	}
}
