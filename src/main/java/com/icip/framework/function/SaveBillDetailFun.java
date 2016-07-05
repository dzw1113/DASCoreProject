package com.icip.framework.function;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
public class SaveBillDetailFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -4218087641817062563L;

	public SaveBillDetailFun(JedisPoolConfig poolConfig, String... fieldArr) {
		super(poolConfig, fieldArr);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String, String> orginalParam = (Map<String, String>) tuple.get(0);

		Map<String, String> map = new HashMap<String, String>();

		for (int i = 0; i < fieldArr.length; i++) {
			map.put(fieldArr[i], tuple.getString(1 + i));
		}
		String newKey = "";
		try {
			String plotId = orginalParam.get("plotId");
			String hid = orginalParam.get("hid");
			String chargeNo = orginalParam.get("chargeNo");

			String year = orginalParam.get("year");
			String month = orginalParam.get("month");

			map.put("roomNo", orginalParam.get("roomNo"));
			map.put("arrears",
					"" + Float.parseFloat(orginalParam.get("arrears")));
			map.put("chargeDuring", orginalParam.get("chargeDuring"));
			map.put("receivedAmount", orginalParam.get("receivedAmount"));
			map.put("payableAmount", orginalParam.get("paymentAmount"));
			map.put("year", year);
			map.put("month", month);
			map.put("billNo", orginalParam.get("billNo"));
			map.put("roomNo", orginalParam.get("roomNo"));
			map.put("ownerName", orginalParam.get("ownerName"));
			if(StringUtils.isBlank(orginalParam.get("chargeName"))){
				map.put("chargeName", "");
			}else{
				map.put("chargeName", orginalParam.get("chargeName"));
			}
			map.put("unitPrice", orginalParam.get("unitPrice"));
			map.put("useNumber", orginalParam.get("useNumber"));
			map.put("lastReading", orginalParam.get("lastReading"));
			map.put("thisReading", orginalParam.get("thisReading"));
			if(StringUtils.isBlank(orginalParam.get("unit"))){
				map.put("unit", "");
			}else{
				map.put("unit", orginalParam.get("unit"));
			}
			newKey = "rt_bill:" + plotId + ":" + hid + ":" + chargeNo + ":"
					+ year + ":" + month;
		} catch (Exception e) {
		}

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
