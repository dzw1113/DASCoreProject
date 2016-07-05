package com.icip.framework.function.yearcollect;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class GetRecYearCollectKeyFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		String[] arr = key.split(":");
		if (arr.length > 4) {
			String cid = arr[1];
			String chargeNo = arr[2];
			String year = arr[3];

			// 账单收款
			String reccy = "cal_recYearCollectByCidChargeYear:" + ":" + cid
					+ ":" + chargeNo + ":" + year;
			String recc = "cal_recYearCollectByCidCharge" + ":" + cid + ":"
					+ chargeNo;
			String recy = "cal_reYearCollectByCidYear" + ":" + cid + ":" + year;

			String newKey = key.replace("cal_receivedTotalByChargeNo",
					"cal_RealAmountByChargeNo");

			collector.emit(new Values(reccy, recc, recy, newKey));
		}
	}
}
