package com.icip.framework.function.common;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/** 
 * @Description: 从redis取指定字符串的值
 * @author  
 * @date 2016年3月29日 下午5:56:25 
 * @update	
 */
public class QueryFieldByRedisFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -8585173489601504387L;

	public QueryFieldByRedisFun(JedisPoolConfig poolConfig,
			String tbKey, String fieldKey) {
		super(poolConfig, tbKey, fieldKey);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			String key = tuple.getString(0);

			String redisVals = jedis.hget(key, fieldKey);
			if(StringUtils.isBlank(redisVals))
				redisVals = "";
			collector.emit(new Values(redisVals));
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}
	
}
