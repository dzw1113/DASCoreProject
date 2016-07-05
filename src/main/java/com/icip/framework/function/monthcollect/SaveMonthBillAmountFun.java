package com.icip.framework.function.monthcollect;

import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.framework.function.common.AbstractRedisConnFun;

/**
 * @Description: TODO
 * @author
 * @date 2016年4月9日 下午4:36:02
 * @update
 */
public class SaveMonthBillAmountFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public SaveMonthBillAmountFun(JedisPoolConfig poolConfig,
			String... fieldArr) {
		super(poolConfig, fieldArr);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		map.put("chargeAmount", "" + tuple.get(1));
		map.put("totalChargeAmount", "" + tuple.get(2));
		map.put("thisMonthAmount", "" + tuple.get(3));
		map.put("totalThisMonthAmount", "" + tuple.get(4));
		map.put("incomeAmount", "" + tuple.get(5));
		map.put("totalIncomeAmount", "" + tuple.get(6));
		map.put("prepaidAmount", "" + tuple.get(7));
		map.put("totalPrepaidAmount", "" + tuple.get(8));
		map.put("totalLateFee", "" + tuple.get(9));
		map.put("arrears", "" + tuple.get(10));
		map.put("totalArrears", "" + tuple.get(11));
		// map.put("historyArrears", "" + tuple.get(13));
		// map.put("totalHistoryArreas", "" + tuple.get(14));
		// map.put("allArreas", "" + tuple.get(15));
		// map.put("allArreasSum", "" + tuple.get(16));

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
			jedis.hmset(newKey, map);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
		collector.emit(new Values());
	}
}
