package com.icip.framework.function.common;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/** 
 * @Description: 
 * @author  dzw
 * @date 2016年3月10日 下午3:31:32 
 * @update	 
 */
public class DRPCConcatTbKeyFun extends BaseFunction {

	private String tbKey;

	public DRPCConcatTbKeyFun(String tbKey) {
		this.tbKey = tbKey;
	}

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String newKey = tuple.getString(0);
		collector.emit(new Values(tbKey + ":" + newKey));
	}

}
