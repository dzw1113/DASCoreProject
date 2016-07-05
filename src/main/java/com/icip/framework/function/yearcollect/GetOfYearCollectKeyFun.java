package com.icip.framework.function.yearcollect;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class GetOfYearCollectKeyFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		String[] arr = key.split(":");
		if (arr.length > 4) {
			String cid = arr[1];
			String year = arr[3];

			// 滞纳金
			String ofcc = "cal_ofYearCollectByCidCharge:" + ":" + cid + ":"
					+ year;
			String ofcy = "cal_ofYearCollectByCidYear:" + ":" + cid;

			String ofKey = key.replace("cal_receivedTotalByChargeNo",
					"cal_offsetTotalByChargeNo");

			collector.emit(new Values(ofcc, ofcy, ofKey));
		}
	}
}
