package com.icip.framework.function.common;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * @Description: 从redis取数据
 * @author dzw
 * @date 2016年3月10日 下午3:31:32
 * @update
 */
public class QueryStrByRedisFun extends AbstractRedisConnFun {

	public QueryStrByRedisFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	private static final long serialVersionUID = 7966578941888674109L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		String key = tuple.getString(0);
		Jedis jedis = null;
		try {
			jedis = getJedis();
			String redisVals = jedis.get(key);
			Float val = 0f;
			if (null != redisVals) {
				try {
					redisVals = redisVals.replace("[", "").replace("]", "");
					val = Float.parseFloat(redisVals.split(",")[1]);
				} catch (Exception e) {
					LOG.error("解析出错!", e);
					redisVals = "";
				}
			}
			collector.emit(new Values(val));
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
				jedis = null;
			}
		}
	}

}
