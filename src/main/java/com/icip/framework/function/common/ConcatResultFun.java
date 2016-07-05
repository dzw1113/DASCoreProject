package com.icip.framework.function.common;

import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/** 
 * @Description: 组装结果集(vt_----rt_)并保存
 * @author  
 * @date 2016年3月29日 下午5:05:34 
 * @update	
 */
public class ConcatResultFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public ConcatResultFun(JedisPoolConfig poolConfig,
			String... fieldArr) {
		super(poolConfig,fieldArr);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String orginalKey = tuple.getString(0);
		Map<String,String> orginalParam = (Map<String, String>) tuple.get(1);
		for(int i = 0; i < fieldArr.length; i++){
			orginalParam.put(fieldArr[i], tuple.getString(2 + i));
		}
		String newKey = orginalKey.replaceFirst("vt_", "rt_");
		
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.hmset(newKey, orginalParam);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
