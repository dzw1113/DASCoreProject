package com.icip.framework.function;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.jdbc.QueryDataUtil;
import com.icip.das.core.kafka.MapKafkaProducer;

public class HandlerSQLDataFun extends BaseFunction {

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sql = tuple.getString(0);
		TableConfigBean conf = (TableConfigBean) tuple.get(1);
		if (!StringUtils.isEmpty(sql)) {
			List<Map<String, String>> data = QueryDataUtil.query(
					conf.getSourceName(), sql);
			MapKafkaProducer producer = new MapKafkaProducer(
					conf.getTableName(), data, true);
			producer.send();
		}
	}

}
