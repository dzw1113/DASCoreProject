package com.icip.das.core.test.function;

import java.util.List;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.framework.function.common.AbstractRedisConnFun;

/**
 * 
 * @Description: 获取主账单信息
 * @author
 * @date 2016年3月10日 下午5:16:04
 * @update
 */
public class GetBillFunction extends AbstractRedisConnFun {

	public GetBillFunction(JedisPoolConfig poolConfig, 
			String[] fieldArr) {
		super(poolConfig, fieldArr);
	}

	private static final long serialVersionUID = -1455995763245459942L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.debug("------>根据队列编号获取账单主表信息!");
		String key = tuple.getString(0);
		Jedis jedis = null;
		try {
			jedis = getJedis(getRedisState());
			List<String> redisVals = jedis.hmget(key, fieldArr);
			collector.emit(new Values(redisVals));
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
