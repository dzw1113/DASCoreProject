package com.icip.framework.redis;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import storm.trident.operation.BaseFunction;

public abstract class RedisBaseFunction extends BaseFunction {

	private static final long serialVersionUID = -6234993476507606340L;

	private JedisPoolConfig poolConfig;

	public RedisBaseFunction(String redisHost, Integer redisPort,
			String password) {
		poolConfig = new JedisPoolConfig.Builder().setHost(redisHost)
				.setPort(redisPort).setPassword(password).build();
	}

	public JedisPoolConfig getPoolConfig() {
		return poolConfig;
	}

	public void setPoolConfig(JedisPoolConfig poolConfig) {
		this.poolConfig = poolConfig;
	}

}
