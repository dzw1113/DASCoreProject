package com.icip.framework.function.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月14日 上午10:29:06 
 * @update	
 */
public class QueryDataByRedisAllowBlankFun extends AbstractRedisConnFun {

	public QueryDataByRedisAllowBlankFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	private static final long serialVersionUID = 7966578941888674109L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		Jedis jedis = null;
		try {
			jedis = getJedis();
			Map<String, String> redisVals = jedis.hgetAll(key);
			if (null != redisVals && redisVals.size() > 0) {
				collector.emit(new Values(redisVals));
				return;
			}
			if (null == redisVals || redisVals.size()==0){
				collector.emit(new Values(new HashMap<String,String>()));
			}

		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}
	
}
