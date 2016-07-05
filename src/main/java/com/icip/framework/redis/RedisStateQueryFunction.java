package com.icip.framework.redis;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.redis.trident.state.RedisState;

import redis.clients.jedis.Jedis;

public class RedisStateQueryFunction extends
		AbstractRedisStateQueryFunction<RedisState> {
	private static final long serialVersionUID = 6023724685649455093L;

	/**
	 * Constructor
	 * 
	 * @param lookupMapper
	 *            mapper for querying
	 */
	public RedisStateQueryFunction(RedisLookupMapper lookupMapper) {
		super(lookupMapper);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected List<String> retrieveValuesFromRedis(RedisState state,
			List<String> keys) {
		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			List<String> redisVals;

			String[] keysForRedis = keys.toArray(new String[keys.size()]);
			switch (dataType) {
			case STRING:
				redisVals = jedis.mget(keysForRedis);
				break;
			case HASH:
				redisVals = jedis.hmget(additionalKey, keysForRedis);
				break;
			case LIST:
				List<String> popList = new ArrayList<String>();
				popList.add(jedis.rpop(additionalKey));
				redisVals = popList;
				break;
			default:
				throw new IllegalArgumentException(
						"Cannot process such data type: " + dataType);
			}

			return redisVals;
		} finally {
			if (jedis != null) {
				state.returnJedis(jedis);
			}
		}
	}

}
