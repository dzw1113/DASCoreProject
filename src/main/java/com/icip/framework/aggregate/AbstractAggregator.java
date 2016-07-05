package com.icip.framework.aggregate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.CombinerAggregator;

/**
 * 
 * @Description: 抽象提取sum集合器
 * @author
 * @date 2016年3月19日 上午12:27:35
 * @update
 */
public abstract class AbstractAggregator implements CombinerAggregator<Double> {

	private static final long serialVersionUID = 3158898645116490916L;

	protected static final Logger LOG = LoggerFactory
			.getLogger(AbstractAggregator.class);

	protected String clacKey;

	protected String fieldKey;

	protected String fieldVal;

	public AbstractAggregator(String clacKey) {
		this.clacKey = clacKey;
	}

	public AbstractAggregator(String clacKey, String fieldKey, String fieldVal) {
		this.clacKey = clacKey;
		this.fieldKey = fieldKey;
		this.fieldVal = fieldVal;
	}
}
