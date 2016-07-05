package com.icip.framework.aggregate;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class OnlyAggregatorImpl implements CombinerAggregator<Map> {

	public OnlyAggregatorImpl() {
	}

	private static final long serialVersionUID = -7034885364836795070L;

	private static long ct = 0;

	@Override
	public Map init(TridentTuple tuple) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		return map;
	}

	@Override
	public Map combine(Map val1, Map val2) {
		System.err.println(val1);
		return null;
	}

	@Override
	public Map zero() {
		return new HashMap<String, String>();
	}

}
