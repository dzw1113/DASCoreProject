package com.icip.framework.function.common;

import java.util.Iterator;
import java.util.Set;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 从redis取key
 * @author dzw
 * @date 2016年3月10日 下午3:31:32
 * @update
 */
public class QueryKeyByRedisFun extends AbstractRedisConnFun {

	public QueryKeyByRedisFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	private static final long serialVersionUID = 7966578941888674109L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String pattern = tuple.getString(0);
		Jedis jedis = null;
		try {
			jedis = getJedis();
			Set<String> set = jedis.keys(pattern);
			Iterator<String> it = set.iterator();
			while (it.hasNext()) {
				String key = it.next();
				collector.emit(new Values(key));
			}
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
