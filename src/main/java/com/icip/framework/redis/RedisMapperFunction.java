package com.icip.framework.redis;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class RedisMapperFunction extends RedisBaseFunction{

	public RedisMapperFunction(String redisHost, Integer redisPort,
			String password) {
		super(redisHost, redisPort, password);
	}

	private static final long serialVersionUID = 53016365599249389L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		
	}

}
