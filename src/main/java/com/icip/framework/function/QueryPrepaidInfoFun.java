package com.icip.framework.function;

import java.util.List;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: 查询预缴信息表中预缴金额（对应单个费项）
 * @author  
 * @date 2016年4月12日 下午4:37:27 
 * @update	
 */
public class QueryPrepaidInfoFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -8585173489601504387L;

	public QueryPrepaidInfoFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);

		String fieldKey = "vt_prepaid_info" + ":" + key; 
		
		Jedis jedis = null;
		try {
			jedis = getJedis();
			List<String> redisVals = jedis.hmget(fieldKey, "prepaidAmount");
			if(null == redisVals || redisVals.size() == 0){
				collector.emit(new Values("0"));
				return;
			}
			if(redisVals.get(0) == null || redisVals.get(0).equals("null")){
				collector.emit(new Values("0"));
				return;
			}
			collector.emit(new Values(redisVals.get(0)));
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
