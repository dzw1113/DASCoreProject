package com.icip.das.core.test.function;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @Description: 计算尚欠款
 * @author
 * @date 2016年3月10日 下午5:34:13
 * @update
 */
public class GetOwnAmountFunction extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	private static final Logger LOG = LoggerFactory.getLogger(GetOwnAmountFunction.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.debug("------>计算尚欠款!");
		List<String> list = (List<String>) tuple.get(0);
		Float paymentAmount = Float.parseFloat("".equals(list.get(2)) ? "0"
				: list.get(2));
		Float receivedAmount = Float.parseFloat("".equals(list.get(17)) ? "0"
				: list.get(17));
		List<Object> values = new ArrayList<>();
		values.add(paymentAmount - receivedAmount);
		collector.emit(new Values(values));
	}

}
