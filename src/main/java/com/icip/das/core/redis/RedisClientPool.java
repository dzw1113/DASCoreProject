package com.icip.das.core.redis;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisClientPool implements Serializable {

	private static final long serialVersionUID = -7619021618829543396L;

	private static redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

	public static RedisClientPool redisClientPool = getInstance();

	private static JedisPool jedisPool;

	public static synchronized RedisClientPool getInstance() {
		if (null == redisClientPool) {
			redisClientPool = new RedisClientPool();
		}
		return redisClientPool;
	}

	public static Jedis getResource() {
		return jedisPool.getResource();
	}

	public static JedisPool getPool() {
		return jedisPool;
	}

	public static void close(Jedis redis) {
		if (redis != null) {
			redis.close();
		}
	}

	public static JedisPoolConfig getStormRedisConfig() {
		String host = RedisPropertiesUtil.PRO_INSTANCE.getProperties()
				.get("redis.host").toString();
		int port = Integer.parseInt(RedisPropertiesUtil.PRO_INSTANCE
				.getProperties().get("redis.port").toString());
		int timeout = Integer
				.parseInt((String) RedisPropertiesUtil.PRO_INSTANCE
						.getProperties().get("redis.timeout"));// 60000;
		String password = RedisPropertiesUtil.PRO_INSTANCE.getProperties()
				.get("redis.password").toString();
		JedisPoolConfig.Builder builder = new JedisPoolConfig.Builder()
				.setHost(host).setPort(port).setTimeout(timeout);
		if (!StringUtils.isEmpty(password)) {
			builder = builder.setPassword(password);
		}
		JedisPoolConfig poolConfig = builder.build();
		return poolConfig;
	}

	public static redis.clients.jedis.JedisPoolConfig getRedisDefaultConfig() {
		int maxIdle = Integer.parseInt(RedisPropertiesUtil.PRO_INSTANCE
				.getProperties().get("redis.maxIdle").toString());
		int maxWaitMillis = Integer
				.parseInt((String) RedisPropertiesUtil.PRO_INSTANCE
						.getProperties().get("redis.maxWait"));// 60000;
		DEFAULT_POOL_CONFIG.setMaxIdle(maxIdle);
		DEFAULT_POOL_CONFIG.setMaxTotal(6000);// 设置最大连接数
		DEFAULT_POOL_CONFIG.setTestOnBorrow(false);
		DEFAULT_POOL_CONFIG.setMaxWaitMillis(maxWaitMillis);
		return DEFAULT_POOL_CONFIG;
	}

	private RedisClientPool() {
		if (null == jedisPool) {
			init();
		}
	}

	/**
	 * 初始化jedis连接池
	 */
	private static void init() {
		String host = RedisPropertiesUtil.PRO_INSTANCE.getProperties()
				.get("redis.host").toString();
		int port = Integer.parseInt(RedisPropertiesUtil.PRO_INSTANCE
				.getProperties().get("redis.port").toString());
		int timeout = Integer
				.parseInt((String) RedisPropertiesUtil.PRO_INSTANCE
						.getProperties().get("redis.timeout"));// 60000;

		String password = (String) RedisPropertiesUtil.PRO_INSTANCE
				.getProperties().get("redis.password");
		if (StringUtils.isBlank(password)) {
			// 构造连接池
			jedisPool = new JedisPool(getRedisDefaultConfig(), host, port, timeout);// TODO
		} else {
			jedisPool = new JedisPool(getRedisDefaultConfig(), host, port, timeout,
					password);
		}
	}

}
