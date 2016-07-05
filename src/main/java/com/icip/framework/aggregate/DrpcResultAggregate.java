package com.icip.framework.aggregate;

import java.util.Map;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @Description: 远程调用聚合器
 * @author
 * @date 2016年3月19日 下午2:42:28
 * @update
 */
public class DrpcResultAggregate implements Aggregator<Object> {

	private static final long serialVersionUID = -3860858532219671228L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Object init(Object batchId, TridentCollector collector) {
		return null;
	}

	@Override
	public void aggregate(Object val, TridentTuple tuple,
			TridentCollector collector) {
		collector.emit(tuple.getValues());
	}

	@Override
	public void complete(Object val, TridentCollector collector) {
	}

}
