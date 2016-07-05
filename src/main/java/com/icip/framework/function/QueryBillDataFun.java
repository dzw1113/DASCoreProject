package com.icip.framework.function;

import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月13日 下午3:58:11 
 * @update	
 */
public class QueryBillDataFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -8585173489601504387L;

	public QueryBillDataFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		String fieldKey = "rt_bill" + ":" + key; 
		
		Jedis jedis = null;
		try {
			jedis = getJedis();
			Map<String,String> redisVals = jedis.hgetAll(fieldKey);
			if(null == redisVals || redisVals.size() == 0){
				return;
			}
			collector.emit(new Values(redisVals));
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
