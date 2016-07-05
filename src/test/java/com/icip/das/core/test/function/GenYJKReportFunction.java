package com.icip.das.core.test.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.framework.function.common.AbstractRedisConnFun;

/**
 * 
 * @Description: 生成报表
 * @author
 * @date 2016年3月10日 下午5:15:42
 * @update
 */
public class GenYJKReportFunction extends AbstractRedisConnFun {

	public GenYJKReportFunction(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	private static final long serialVersionUID = -4218087641817062563L;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LOG.debug("--------------------->组装报表结果");
		// TODO 根据业务场景
		// 主表
		List<String> billList = (List<String>) tuple.get(0);
		List<String> dtList = (List<String>) tuple.get(1);
		List<String> dateList = (List<String>) tuple.get(2);
		List<String> houseInfoList = (List<String>) tuple.get(3);
		List<String> chargeNameList = (List<String>) tuple.get(4);
		List<Object> oweAmountList = (List<Object>) tuple.get(5);
		List<Object> values = new ArrayList<>();

		Jedis jedis = null;
		try {
			jedis = getJedis(getRedisState());

			Long id = jedis.incr("YJK_INCR_NUM");

			// TODO 全部存起来
			Map<String, String> map = new HashMap<String, String>();
			// 年
			map.put("YEAR", dateList.get(0));
			// 月
			map.put("MONTH", dateList.get(1));
			// 房间名称
			map.put("HOUSE_INFO", houseInfoList.get(0));
			// 住户名称
			map.put("OWNER_NAME", billList.get(13));
			// 项目名称
			map.put("CHARGE_NAME", "" + chargeNameList.get(0));
			// 收费期间
			map.put("PAYMENT_DATE", dtList.get(11));
			// 单价
			map.put("UNIT_PRICE", dtList.get(5));
			// 应交款
			map.put("PAYMENT_AMOUNT", dtList.get(2));
			// 上期读数
			map.put("LAST_READING", dtList.get(13));
			// 本期读数
			map.put("THIS_READING", dtList.get(14));
			// 面积/用量
			map.put("USE_NUMBER", dtList.get(6));
			// 单位 没有！TODO
			map.put("UNIT", "没有单位");
			// 已交款
			map.put("RECEIVED_AMOUNT", dtList.get(17));
			// 尚欠款
			map.put("OWE_AMOUNT", "" + oweAmountList.get(0));
			// 备注
			map.put("CHARGE_MARK", dtList.get(21));
			// 说明
			map.put("MARK", "没有说明！");
			// 单据编号
			map.put("RECEIPT_NUMBER", "没有单据编号");
			try {
				jedis.hmset("YJK_REPORT:" + id, map);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}

		collector.emit(new Values(values));
	}
}
