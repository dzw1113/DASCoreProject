package com.icip.framework.function;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.framework.function.common.AbstractRedisConnFun;

public class QueryProprietorfoFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = 7966578941888674109L;

	public QueryProprietorfoFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		Jedis jedis = null;
		try {
			jedis = getJedis();
			List<String> redisVals = jedis.hmget("", "unit");
			collector.emit(new Values(""));
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
