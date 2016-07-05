package com.icip.framework.function.common;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class RedisPublishFun extends AbstractRedisConnFun {

	public RedisPublishFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	public RedisPublishFun() {
		super();
	}

	private static final long serialVersionUID = 7966578941888674109L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		Jedis jedis = null;
		try {
			String channel = key.substring(0, key.indexOf(":"));
			jedis = getJedis();
			jedis.publish(channel, key);
			collector.emit(new Values());
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
