package com.icip.framework.function.yearcollect;

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
public class SaveAnnualChargeAmountFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public SaveAnnualChargeAmountFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		Object ob = tuple.get(1);
		if (ob == null) {
			ob = "0";
		}

		Map<String, String> mapNk = new HashMap<String, String>();
		String arr[] = key.split(":");
		mapNk.put("plotId", arr[1]);
		mapNk.put("chargeNo", arr[2]);
		mapNk.put("year", arr[3]);
		mapNk.put("annualChargeAmount", "" + ob);

		String newKey = "rt_YearBillAmount";
		String plotId = arr[1];
		String chargeNo = arr[2];
		String year = arr[3];
		newKey = newKey + ":" + plotId + ":" + chargeNo + ":" + year;

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
