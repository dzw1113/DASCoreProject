package com.icip.framework.function.common;

import java.util.List;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 根据key从redis查询某些字段 hget key fields1 fields2
 * @author
 * @date 2016年3月10日 下午5:35:52
 * @update
 */
public class QueryFieldsByRedisFun extends AbstractRedisConnFun {

	public QueryFieldsByRedisFun(JedisPoolConfig poolConfig,
			String tbKey, String... fieldArr) {
		super(poolConfig, tbKey, fieldArr);
	}

	private static final long serialVersionUID = -4218087641817062563L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			String key = tuple.getString(0);

			List<String> redisVals = jedis.hmget(key, fieldArr);
			collector.emit(new Values(redisVals));
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
