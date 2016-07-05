package com.icip.framework.function;

import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 联合账单和账单主表：组合房号、年、月、尚欠款
 * @author
 * @date 2016年4月11日 上午10:08:55
 * @update
 */
public class BillConcatMapFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		Map<String, String> map1 = (Map<String, String>) tuple.get(1);
		map.putAll(map1);
		map.put("year", map.get("billDate").substring(0, 4));
		map.put("month", map.get("billDate").substring(4, 6));
		map.put("roomNo",
				map.get("areaNo") + map.get("unitNo") + map.get("roomNo"));
		map.put("arrears",
				""
						+ (Float.parseFloat(map.get("paymentAmount")) - Float
								.parseFloat(map.get("paymentAmount"))));
		map.put("chargeDuring", map.get("startDate") + "-" + map.get("endDate"));
		collector.emit(new Values(map));
	}

}
