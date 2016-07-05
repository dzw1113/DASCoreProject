package com.icip.framework.function.yearcollect;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 拆分key，得小区、费项、年/小区、年
 * @author dzw
 * @date 2016年3月10日 下午5:35:52
 * @update
 */
public class YearCollectEmitKeyFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		String[] arr = key.split(":");
		if (arr.length > 4) {
			collector.emit(new Values("cal_receivedTotalByChargeNoYear:"
					+ arr[1] + ":" + arr[2] + ":" + arr[3],
					"cal_receivedTotalByYear:" + arr[1] + ":" + arr[3],
					"cal_receivedTotalByAllYearCid:" + arr[1]));
		}
	}

}
