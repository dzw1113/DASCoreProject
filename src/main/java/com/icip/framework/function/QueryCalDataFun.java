package com.icip.framework.function;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月1日 上午11:01:37 
 * @update	
 */
public class QueryCalDataFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -8585173489601504387L;

	public QueryCalDataFun(JedisPoolConfig poolConfig,
			String fieldKey) {
		super(poolConfig, fieldKey);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			String key = tuple.getString(0);

			String redisVal = jedis.get(fieldKey + ":" + key);
			if(StringUtils.isBlank(redisVal)){
				redisVal = "0";
			}else{
				//截掉标识位
				redisVal = redisVal.split(",")[1].split("]")[0];
			}
//			LOG.info("CT---->"+ct++);
			collector.emit(new Values(redisVal));
		}catch(Exception ex){
			System.out.println(ex);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
