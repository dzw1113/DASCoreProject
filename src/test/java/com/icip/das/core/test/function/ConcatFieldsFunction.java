package com.icip.das.core.test.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 组装多个字段为一个字段
 * @author
 * @date 2016年3月29日 上午10:36:28
 * @update
 */
public class ConcatFieldsFunction extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String newKey = "";
		for (int i = 0; i < tuple.getValues().size(); i++) {
			if (i == 0) {
				newKey = "" + tuple.getValues().get(i);
			} else {
				newKey = newKey + "|" + tuple.getValues().get(i);
			}
		}
		collector.emit(new Values(newKey));
	}

}
