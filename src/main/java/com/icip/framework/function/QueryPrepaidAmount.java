package com.icip.framework.function;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: 查询预缴信息表中单户所有费项总和
 * @author  
 * @date 2016年4月5日 下午3:21:09 
 * @update	
 */
public class QueryPrepaidAmount  extends AbstractRedisConnFun {

	private static final long serialVersionUID = -8585173489601504387L;

	public QueryPrepaidAmount(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String,String> map = (Map<String, String>) tuple.get(0);

		String cid = map.get("cid");
		String hid = map.get("hid");
		String year = map.get("year");
		String month = map.get("month");
		String fieldKey = "vt_prepaid_info" + cid + ":" + hid + ":*:" + year + ":" + month; 
		
		Float amount = Float.valueOf("0");
		Jedis redisPipeClient = null;
		try{
			redisPipeClient = RedisClientPool.getResource();
			Pipeline pipe = redisPipeClient.pipelined();
			Set<String> keys = redisPipeClient.keys(fieldKey);

			Map<String,Response<Map<String, String>>> responses = new HashMap<String,Response<Map<String, String>>>(keys.size()); 
			for(String key : keys) { 
				responses.put(key, pipe.hgetAll(key)); 
			} 
			pipe.sync(); 
			for(String k : responses.keySet()) {
				String temp = responses.get(k).get().get("prepaidAmount");
				if(!StringUtils.isBlank(temp)){
					amount = amount + Float.valueOf(temp);
				}
			} 
		}catch(Exception ex){
			System.out.println(ex);
		}finally{
			if(null != redisPipeClient){
				redisPipeClient.close();
			}
		}
		collector.emit(new Values(amount.toString()));
	}
		
}
