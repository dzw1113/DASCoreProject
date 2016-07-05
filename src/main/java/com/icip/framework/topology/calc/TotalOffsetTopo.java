package com.icip.framework.topology.calc;

import java.util.Random;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.aggregate.SumAggregatorImpl;
import com.icip.framework.constans.DASConstants;
import com.icip.framework.function.common.ConcatRedisKeyFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.spout.RedisPubSubSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/**
 * @Description: 冲销总额统计(bs_offset_jouranls,bs_pay_bills,bs_bill_details) 按
 *               cid/year/month cid/hid/year/month cid/chargeNo/year/month
 *               cid/hid/chargeNo/year/month
 * @author
 * @date 2016年3月31日 下午4:40:13
 * @update
 */
public class TotalOffsetTopo extends AbstractBaseTopo {

	private static final long serialVersionUID = 5411742705538906450L;

	public TotalOffsetTopo(String topologyName) {
		super(topologyName);
	}

	public static final String topic = "vt_totalOffsetAmount";
	public static final String topologyName = "Calc_TotalOffset"
			+ new Random().nextInt(100);
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new TotalOffsetTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout("vt_totalOffsetAmount");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		Stream streamTotal = stream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig), new Fields(
						DASConstants.DATA)).parallelismHint(10);

		// -----冲销 cid/year/month统计
		StateFactory offsetByCidFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_offsetTotalByCid",
								new String[] { "plotId", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(offsetByCidFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("offsetAmount"),
						new Fields("offsetTotalByCid"));

		// -----冲销 hid/year/month统计
		StateFactory offsetByHidFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_offsetTotalByHid",
								new String[] { "plotId", "hid", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(offsetByHidFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("offsetAmount"),
						new Fields("offsetTotalByHid"));

		// -----冲销 cid/chargeNo/year/month统计
		StateFactory offsetByChargeNoFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_offsetTotalByChargeNo",
								new String[] { "plotId", "chargeNo", "year",
										"month" }), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(offsetByChargeNoFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("offsetAmount"),
						new Fields("offsetByChargeNo"));

		// -----冲销 cid/hid/chargeNo/year/month统计
		StateFactory offsetByHidAndChargeNoFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_offsetTotalByHidAndChargeNo",
								new String[] { "plotId", "hid", "chargeNo",
										"year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(offsetByHidAndChargeNoFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("offsetAmount"),
						new Fields("offsetByHidAndChargeNo"));

		return topology.build();
	}

}
