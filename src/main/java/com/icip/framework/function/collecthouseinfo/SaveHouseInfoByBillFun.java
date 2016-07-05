package com.icip.framework.function.collecthouseinfo;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.framework.function.common.AbstractRedisConnFun;

public class SaveHouseInfoByBillFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public SaveHouseInfoByBillFun(JedisPoolConfig poolConfig,
			String... fieldArr) {
		super(poolConfig, fieldArr);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		Map<String, String> newMap = new HashMap<String, String>();

		String plotId = map.get("plotId");
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

		String ownerName = StringUtils.isEmpty(map.get("ownerName")) ? "" : map
				.get("ownerName");
		newMap.put("ownerName", ownerName);
		String ownerMobile = StringUtils.isEmpty(map.get("ownerMobile")) ? ""
				: map.get("ownerMobile");
		newMap.put("ownerMobile", ownerMobile);
		String area = StringUtils.isEmpty(map.get("useNumber")) ? "" : map
				.get("useNumber");
		newMap.put("area", area);

		String newKey = "vt_houseinfo" + ":" + plotId + ":" + hid + ":"
				+ chargeNo;
		newKey = newKey + ":" + year + ":" + month;

		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.hmset(newKey, newMap);
		} catch (Exception e) {
			System.err.println("插入失败！-----》" + newMap);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}

}
