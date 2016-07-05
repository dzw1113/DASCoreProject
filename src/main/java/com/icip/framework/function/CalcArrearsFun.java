package com.icip.framework.function;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class CalcArrearsFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	private static final Logger LOG = LoggerFactory
			.getLogger(CalcArrearsFun.class);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.debug("------>计算尚欠款!");
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		if (map != null && map.size() > 0) {
			Float paymentAmount = Float.parseFloat((String) ("".equals(map
					.get("paymentAmount")) ? "0" : map.get("paymentAmount")));
			Float receivedAmount = Float.parseFloat((String) ("".equals(map
					.get("receivedAmount")) ? "0" : map.get("receivedAmount")));
			List<Object> values = new ArrayList<>();
			values.add(paymentAmount - receivedAmount);
			collector.emit(new Values(values));
		}

	}

}
