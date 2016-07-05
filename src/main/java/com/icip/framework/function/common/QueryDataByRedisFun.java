package com.icip.framework.function.common;

import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;

/**
 * @Description: 从redis取数据
 * @author dzw
 * @date 2016年3月10日 下午3:31:32
 * @update
 */
public class QueryDataByRedisFun extends AbstractRedisConnFun {

	public QueryDataByRedisFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	public QueryDataByRedisFun() {
		super();
	}

	private static final long serialVersionUID = 7966578941888674109L;


	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		if (key == null) {
			System.err.println("没有找到数据！");
			throw new FailedException();
		}
		Jedis jedis = null;
		try {

			jedis = getJedis();
			Map<String, String> redisVals = jedis.hgetAll(key);
			if (null != redisVals && redisVals.size() > 0) {
				collector.emit(new Values(redisVals));
			}

			if (null == redisVals || redisVals.size() == 0) {
//				LOG.error("-----找不到redis数据------>ERROR:" + key);
				throw new FailedException();
			}
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
