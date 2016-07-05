package com.icip.framework.function.common;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.trident.state.RedisState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.redis.RedisClientPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import storm.trident.operation.BaseFunction;

/**
 * 
 * @Description: 抽象提取redis连接类
 * @author
 * @date 2016年3月10日 下午5:28:23
 * @update
 */
public abstract class AbstractRedisConnFun extends BaseFunction {

	private static final long serialVersionUID = 8392569087692959151L;

	public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = RedisClientPool
			.getRedisDefaultConfig();

	public static final JedisPool jedisPool = RedisClientPool.getPool();

	protected static final Logger LOG = LoggerFactory
			.getLogger(AbstractRedisConnFun.class);

	protected JedisPoolConfig jedisPoolConfig;
	protected String tbKey;
	protected String[] fieldArr;
	protected String fieldKey;

	public AbstractRedisConnFun(JedisPoolConfig poolConfig, String tbKey,
			String[] fieldArr) {
		this.jedisPoolConfig = poolConfig;
		this.tbKey = tbKey;
		this.fieldArr = fieldArr;
	}

	public AbstractRedisConnFun(JedisPoolConfig poolConfig) {
		this.jedisPoolConfig = poolConfig;
	}

	public AbstractRedisConnFun() {
		this.jedisPoolConfig = RedisClientPool.getStormRedisConfig();
	}

	public AbstractRedisConnFun(JedisPoolConfig poolConfig, String fieldKey) {
		this.fieldKey = fieldKey;
		this.jedisPoolConfig = poolConfig;
	}

	public AbstractRedisConnFun(JedisPoolConfig poolConfig, String tbKey,
			String fieldKey) {
		this.fieldKey = fieldKey;
		this.jedisPoolConfig = poolConfig;
		this.tbKey = tbKey;
	}

	public AbstractRedisConnFun(JedisPoolConfig poolConfig, String[] fieldArr) {
		this(poolConfig, null, fieldArr);
	}

	public AbstractRedisConnFun(String fieldKey) {
		this(RedisClientPool.getStormRedisConfig(), fieldKey);
	}

	public RedisState getRedisState() {
		return new RedisState(jedisPool);
	}

	public Jedis getJedis(RedisState redisState) {
		return jedisPool.getResource();
	}

	public Jedis getJedis() {
		return this.getJedis(null);
	}

	public void returnJedis(Jedis jedis) {
		jedis.close();
	}
}
