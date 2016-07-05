package com.icip.framework.topology.calc;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import scala.util.Random;
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
 * @Description 本月所有滞纳金
 * @author
 * @date 2016年3月30日 上午10:04:39
 * @update
 */
public class TotalLateFeeTopo extends AbstractBaseTopo {

	public TotalLateFeeTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topic = "vt_totle_late_fee";
	public static final String topologyName = "Calc_TotalLateFee_TOPO"
			+ new Random().nextInt(10000);
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new TotalLateFeeTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout("vt_totle_late_fee");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		// --------------------筛选
		Stream streamTotalLateFee = stream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig), new Fields(
						DASConstants.DATA)).parallelismHint(10);
		// ------------本月所有滞纳金之和 小区、房屋、月份
		StateFactory totleByHidFactory = getStrFactory(poolConfig);
		streamTotalLateFee
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_LateFeeByHid", "cid", "hid",
								"year", "month"), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(totleByHidFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("lateFee"), new Fields("lateFee"));

		StateFactory lfByChargeNoFactory = getStrFactory(poolConfig);
		streamTotalLateFee
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_LateFeeByChargeNo", "cid",
								"chargeNo", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(lfByChargeNoFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("lateFee"), new Fields("lateFee"));

		StateFactory lfByPlotIdFactory = getStrFactory(poolConfig);
		streamTotalLateFee
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_LateFeeByPlotId", "cid",
								"year", "month"), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(lfByPlotIdFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("lateFee"), new Fields("lateFee"));

		StateFactory factory = getStrFactory(poolConfig);
		streamTotalLateFee
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_LateFeeByYear", "cid",
								"year"), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(factory, new Fields(DASConstants.DATA),
						new SumAggregatorImpl("lateFee"), new Fields("lateFee"));

		StateFactory factoryhc = getStrFactory(poolConfig);
		streamTotalLateFee
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_LateFeeByHidChargeNo",
								"cid", "hid", "chargeNo", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(factoryhc, new Fields(DASConstants.DATA),
						new SumAggregatorImpl("lateFee"), new Fields("lateFee"));
		return topology.build();
	}

}