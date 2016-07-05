package com.icip.framework.function.common;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/** 
 * @Description: 根据费项编号查费项名称,没有对应的返回空字符串
 * @author  
 * @date 2016年3月31日 下午7:56:08 
 * @update	
 */
public class QueryChargeNameByChargeNo extends AbstractRedisConnFun  {

	private static final long serialVersionUID = -322622863251623117L;
	
	/**
	 *	@see  com.icip.framework.function.common.QueryChargeNameByChargeNo
	 */
	public QueryChargeNameByChargeNo(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String chargeNo = tuple.getString(0);
		String redisKey = "bd_charge_item:" + chargeNo;
		
		Jedis jedis = null;
		String chargeName = null;
		try {
			jedis = getJedis();
			chargeName = jedis.hget(redisKey, "chargeName");
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
		
		if(StringUtils.isBlank(chargeName)){
			chargeName = "";
		}
		
		collector.emit(new Values(chargeName));
	}

}
