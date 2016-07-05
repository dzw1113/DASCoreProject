package com.icip.das.core.test.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.framework.function.common.AbstractRedisConnFun;

/**
 * 
 * @Description: 获取明细表key
 * @author
 * @date 2016年3月10日 下午5:33:07
 * @update
 */
public class GetDtPrimaryKeyFunction extends AbstractRedisConnFun {

	public GetDtPrimaryKeyFunction(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	private static final long serialVersionUID = -1455995763245459942L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.debug("------>获取账单明细表key!");
		List<String> list = (List<String>) tuple.get(0);
		Jedis jedis = null;
		try {
			jedis = getJedis(getRedisState());
			Set<String> set = jedis.keys("bs_bill_details:" + "*:"
					+ list.get(0));
			Iterator<String> it = set.iterator();

			while (it.hasNext()) {
				String str = it.next();
				List<Object> values = new ArrayList<Object>();
				values.add(str);
				collector.emit(values);
			}
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
