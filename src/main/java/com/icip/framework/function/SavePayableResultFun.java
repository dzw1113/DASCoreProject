package com.icip.framework.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: 查询应交款报表结果保存
 * @author  
 * @date 2016年3月31日 上午10:15:34 
 * @update	
 */
public class SavePayableResultFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;
	
	private String[] valueFieldsArr;

	public SavePayableResultFun(JedisPoolConfig poolConfig,
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
		if(valueFieldsArr.length == 8){
			//按费项 key cid:chargeNo:year:month
			key = "rt_bill_charge" + ":" + tuple.getString(0) + ":" + tuple.getString(1) + ":" + tuple.getString(2) + ":" + tuple.getString(3);
		}else{
			//按费项 房号   cid:hid:year:month
			key = "rt_bill_hid" + ":" + tuple.getString(0) + ":" + tuple.getString(1) + ":" + tuple.getString(2) + ":" + tuple.getString(3);
		}
		
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
