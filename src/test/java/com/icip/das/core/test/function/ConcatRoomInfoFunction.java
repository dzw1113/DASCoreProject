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
 * @Description: 链接区域和连接单元组装新数据
 * @author
 * @date 2016年3月10日 下午5:12:23
 * @update
 */
public class ConcatRoomInfoFunction extends BaseFunction {

	public ConcatRoomInfoFunction() {
	}

	private static final Logger LOG = LoggerFactory.getLogger(ConcatRoomInfoFunction.class);
	
	private static final long serialVersionUID = -1455995763245459942L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.debug("------>组装单元和期号！!");
		List<String> list = (List<String>) tuple.get(0);
		String unitNo = list.get(5);
		String areaNo = list.get(7);
		List<Object> values = new ArrayList<>();
		values.add(areaNo + unitNo);
		collector.emit(new Values(values));
	}

}
