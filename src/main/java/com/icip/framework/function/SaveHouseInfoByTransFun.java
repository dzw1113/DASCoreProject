package com.icip.framework.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.framework.function.common.AbstractRedisConnFun;

public class SaveHouseInfoByTransFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public SaveHouseInfoByTransFun(JedisPoolConfig poolConfig,
			String... fieldArr) {
		super(poolConfig, fieldArr);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		Map<String, String> newMap = new HashMap<String, String>();

		String plotId = map.get("cid");
		String hid = map.get("hid");
		String chargeNo = map.get("chargeNo");
		String year = map.get("year");
		String month = map.get("month");

		String roomNo = map.get("roomNo");
		
		newMap.put("plotId", plotId);
		newMap.put("hid", hid);
		newMap.put("chargeNo", chargeNo);
		newMap.put("year", year);
		newMap.put("month", month);
		
		newMap.put("roomNo", roomNo);

		newMap.put("ownerName", map.get("pname"));
		newMap.put("ownerMobile", map.get("phone"));
		newMap.put("area", map.get("area"));
		

		String newKey = "vt_houseinfo" + ":" + plotId + ":" + hid + ":"
				+ chargeNo;
		newKey = newKey + ":" + year + ":" + month;

		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.hmset(newKey, newMap);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
