package com.icip.framework.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;

public class ColLookupMapper extends AbstractLookupMapper {

	private RedisDataTypeDescription.RedisDataType type;

	public ColLookupMapper(String tableName) {
		super(tableName);
	}

	public ColLookupMapper(String tableName,
			RedisDataTypeDescription.RedisDataType type) {
		super(tableName);
		this.type = type;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 5576695090352243287L;

	@Override
	public RedisDataTypeDescription getDataTypeDescription() {
		return new RedisDataTypeDescription(type, tableName);
	}
}
