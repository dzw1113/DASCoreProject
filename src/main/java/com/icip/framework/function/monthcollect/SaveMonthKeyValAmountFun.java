package com.icip.framework.function.monthcollect;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.framework.function.common.AbstractRedisConnFun;

/**
 * @Description: TODO
 * @author
 * @date 2016年4月9日 下午4:36:02
 * @update
 */
public class SaveMonthKeyValAmountFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public SaveMonthKeyValAmountFun(JedisPoolConfig poolConfig, String fieldKey) {
		super(poolConfig, fieldKey);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		Object val = tuple.get(1);
		if (val == null) {
			val = "0";
		}
		Map<String, String> mapNk = new HashMap<String, String>();
		mapNk.put(fieldKey, "" + val);

		String newKey = "rt_MonthBillAmount";
		String plotId = map.get("plotId");
		String chargeNo = map.get("chargeNo");
		String hid = map.get("hid");
		String year = map.get("year");
		String month = map.get("month");
		newKey = newKey + ":" + plotId + ":" + chargeNo + ":" + hid + ":"
				+ year + ":" + month;

		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.hmset(newKey, mapNk);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}
}
