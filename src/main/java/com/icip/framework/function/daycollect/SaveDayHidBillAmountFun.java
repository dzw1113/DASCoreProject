package com.icip.framework.function.daycollect;

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
public class SaveDayHidBillAmountFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public SaveDayHidBillAmountFun(JedisPoolConfig poolConfig,
			String... fieldArr) {
		super(poolConfig, fieldArr);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		Map<String, String> map = (Map<String, String>) tuple.get(1);
		Object hidTransAmount = tuple.get(2);
		Object chargeTransAmount = tuple.get(3);
		Object hcTransAmount = tuple.get(4);
		if (hidTransAmount == null) {
			hidTransAmount = "0";
		}
		if (chargeTransAmount == null) {
			chargeTransAmount = "0";
		}
		if (hcTransAmount == null) {
			hcTransAmount = "0";
		}

		Map<String, String> newMap = new HashMap<String, String>();
		String newKey = key.replace("vt_trans_detail", "rt_DayHidBillAmount");
		String plotId = map.get("cid");
		String hid = map.get("hid");
		String transTime = map.get("transTime");
		if (transTime.length() >= 8) {
			String year = transTime.substring(0, 4);
			String month = transTime.substring(4, 6);
			String day = transTime.substring(6, 8);
			newKey = newKey + ":" + plotId + ":" + hid + ":" + year + ":"
					+ month + ":" + day;

			newMap.put("plotId", plotId);
			newMap.put("hid", hid);
			newMap.put("roomNo", map.get("roomNo"));
			newMap.put("pid", map.get("pid"));
			newMap.put("pname", map.get("pname"));
			newMap.put("chargeNo", map.get("chargeNo"));
			newMap.put("year", year);
			newMap.put("month", month);
			newMap.put("day", day);
			newMap.put("hidTransAmount", "" + hidTransAmount);
			newMap.put("chargeTransAmount", "" + chargeTransAmount);
			newMap.put("hcTransAmount", "" + hcTransAmount);
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
}
