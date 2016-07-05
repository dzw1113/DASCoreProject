package com.icip.framework.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;

import backtype.storm.tuple.ITuple;

public abstract class AbstractStoreMapper implements RedisStoreMapper {

	private static final long serialVersionUID = -5201903555189150002L;

	private String tableName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public AbstractStoreMapper(String tableName) {
		this.tableName = tableName;
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
