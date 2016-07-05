package com.icip.framework.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: 保存冲销明细结果
 * @author  
 * @date 2016年4月12日 下午5:21:59 
 * @update	
 */
public class SavePrepaidOffsetDetailFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;
	
	private String[] valueFieldsArr;

	public SavePrepaidOffsetDetailFun(JedisPoolConfig poolConfig,
			String... valueFieldsArr) {
		super(poolConfig);
		this.valueFieldsArr = valueFieldsArr;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String,String> param = new HashMap<String,String>();
		for(int i = 0; i < valueFieldsArr.length; i++){
			param.put(valueFieldsArr[i], tuple.getString(i));
		}
		String key = "";
		//  cid:hid:charge:year:month
		key = "rt_prepaid_offset" + ":" + tuple.getString(0) + ":" + tuple.getString(1) + ":" + tuple.getString(2) + ":" + tuple.getString(3) + ":" + tuple.getString(4);
		
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.hmset(key, param);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
