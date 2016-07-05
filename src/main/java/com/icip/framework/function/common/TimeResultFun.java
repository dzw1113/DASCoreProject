package com.icip.framework.function.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * @Description: 打印帮助
 * @author
 * @date 2016年3月10日 下午5:44:38
 * @update
 */
public class TimeResultFun extends BaseFunction {

	private static final Logger LOG = LoggerFactory
			.getLogger(TimeResultFun.class);
	
	private static long counter = 0;
	
	private static final long serialVersionUID = -4505509553569927472L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
		LOG.info("-------counter:"+counter++);
	}
}
