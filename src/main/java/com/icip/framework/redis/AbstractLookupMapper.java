package com.icip.framework.redis;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;

public abstract class AbstractLookupMapper implements RedisLookupMapper {

	private static final long serialVersionUID = -7267483535843499071L;

	protected String tableName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public AbstractLookupMapper(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public List<Values> toTuple(ITuple input, Object value) {
		List<Values> values = new ArrayList<Values>();
		values.add(new Values(getKeyFromTuple(input), value));
		return values;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key"));
	}

	@Override
	public RedisDataTypeDescription getDataTypeDescription() {
		return new RedisDataTypeDescription(
				RedisDataTypeDescription.RedisDataType.HASH, tableName);
	}

	@Override
	public String getKeyFromTuple(ITuple tuple) {
		return tuple.getString(0);
	}

	@Override
	public String getValueFromTuple(ITuple tuple) {
		return "" + tuple.getValue(1);
	}

}
