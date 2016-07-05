package com.icip.framework.function.yearcollect;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class GetRealYearCollectKeyFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		String[] arr = key.split(":");
		if (arr.length > 4) {
			String cid = arr[1];
			String chargeNo = arr[2];
			String year = arr[3];

			// 实收
			String raccy = "cal_realYearCollectByCidChargeYear" + ":" + cid
					+ ":" + chargeNo + ":" + year;
			String racc = "cal_realYearCollectByCidCharge" + ":" + cid + ":"
					+ chargeNo;
			String racy = "cal_realYearCollectByCidYear" + ":" + cid + ":"
					+ year;

			collector.emit(new Values(raccy, racc, racy));
		}
	}
}
