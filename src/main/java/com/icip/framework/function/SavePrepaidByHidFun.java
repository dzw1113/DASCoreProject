package com.icip.framework.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: 预收款查询（按房间项目收）结果保存
 * @author  
 * @date 2016年4月5日 下午3:54:00 
 * @update	
 */
public class SavePrepaidByHidFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;
	
	private String[] valueFieldsArr;

	public SavePrepaidByHidFun(JedisPoolConfig poolConfig,
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
		key = "rt_prepaid_hid" + ":" + tuple.getString(0) + ":" + tuple.getString(1) + ":" + tuple.getString(2) + ":" + tuple.getString(3) + ":" + tuple.getString(4);
		
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
