package com.icip.framework.function.daycollect;

import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class GenDayKeyFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		String cid = map.get("cid");
		String hid = map.get("hid");
		String transTime = map.get("transTime");
		String chargeNo = map.get("chargeNo");

		if (transTime.length() >= 8) {
			String year = transTime.substring(0, 4);
			String month = transTime.substring(4, 6);
			String day = transTime.substring(6, 8);

			// 根据房屋天汇总
			String hdayByHid = "hdayByHid:" + cid + ":" + hid + ":" + year
					+ ":" + month + ":" + day;
			// 根据费项天汇总
			String hdayByChargeNo = "hdayByChargeNo:" + cid + ":" + chargeNo
					+ ":" + year + ":" + month + ":" + day;

			// 根据费项和房屋天汇总
			String hdayByHC = "hdayByHC:" + cid + ":" + hid + ":" + chargeNo
					+ ":" + year + ":" + month + ":" + day;

			collector.emit(new Values(hdayByHid, hdayByChargeNo, hdayByHC));
		}

	}
}
