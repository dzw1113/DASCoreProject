package com.icip.framework.function.daycollect;

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
public class SaveDayBillAmountFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public SaveDayBillAmountFun(JedisPoolConfig poolConfig, String... fieldArr) {
		super(poolConfig, fieldArr);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = tuple.getString(0);
		Map<String, String> map = (Map<String, String>) tuple.get(1);

		String newKey = key.replace( "vt_trans_detail","rt_DayBillAmount");
		String plotId = map.get("cid");
		String chargeNo = map.get("chargeNo");
		String hid = map.get("hid");
		String transTime = map.get("transTime");
		if (transTime.length() >= 8) {
			String year = transTime.substring(0, 4);
			String month = transTime.substring(4, 6);
			String day = transTime.substring(6, 8);
			newKey = newKey + ":" + plotId + ":" + chargeNo + ":" + hid + ":"
					+ year + ":" + month + ":" + day;
			Jedis jedis = null;
			try {
				jedis = getJedis();
				jedis.hmset(newKey, map);
			} finally {
				if (jedis != null) {
					returnJedis(jedis);
				}
			}
		}

	}
}
