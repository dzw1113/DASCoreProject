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
 * @Description: 解析日期
 * @author
 * @date 2016年3月10日 下午5:37:04
 * @update
 */
public class ParseDateFunction extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	private static final Logger LOG = LoggerFactory.getLogger(ParseDateFunction.class);
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.debug("------>解析时间，拆分成：年/月!");
		List<String> list = (List<String>) tuple.get(0);
		String date = list.get(2);
		String year = date.substring(0, 4);
		String month = date.substring(4, 6);
		List<Object> values = new ArrayList<>();
		values.add(year);
		values.add(month);
		collector.emit(new Values(values));
	}

}
