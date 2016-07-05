package com.icip.framework.function;

import java.math.BigDecimal;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class TotalArrFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Object thisRecBillAMCAmount = tuple.get(0);
		if(thisRecBillAMCAmount==null){
			thisRecBillAMCAmount = 0;
		}
		Object arrAMount = tuple.get(1);
		if(arrAMount==null){
			arrAMount = 0;
		}
		BigDecimal x1 = new BigDecimal(""+thisRecBillAMCAmount);
		BigDecimal x2 = new BigDecimal(""+arrAMount);
		collector.emit(new Values(x1.subtract(x2).floatValue()));
	}

}
