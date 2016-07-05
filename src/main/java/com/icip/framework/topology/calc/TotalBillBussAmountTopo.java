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
 * @Description 本月账单收款总金额
 * @author
 * @date 2016年3月30日 上午10:04:39
 * @update
 */
public class TotalBillBussAmountTopo extends AbstractBaseTopo {

	public TotalBillBussAmountTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	// 主题
	public static final String topic = "vt_bill_trans";
	// topogy名称
	public static final String topologyName = "Calc_TotalBillBussAmount_TOPO"
			+ new Random().nextInt(10000);
	// 本地模式还是集群模式
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new TotalBillBussAmountTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout("vt_bill_trans");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		Stream streamTotalPrepaidAmount = stream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig), new Fields(
						DASConstants.DATA)).parallelismHint(10);

		StateFactory totalTMByHidFactory = getStrFactory(poolConfig);

		Stream keyStream = streamTotalPrepaidAmount.each(new Fields(
				DASConstants.DATA), new ConcatRedisKeyFun(
				"cal_RealAmountByHid", "plotId", "hid", "year", "month"),
				new Fields("newKey"));

		keyStream.groupBy(new Fields("newKey")).persistentAggregate(
				totalTMByHidFactory, new Fields(DASConstants.DATA),
				new SumAggregatorImpl("businessAmount"),
				new Fields("businessAmount"));

		StateFactory totalTMByChargeNoFactory = getStrFactory(poolConfig);
		streamTotalPrepaidAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_RealAmountByChargeNo",
								"plotId", "chargeNo", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(totalTMByChargeNoFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		StateFactory totalTMFactory = getStrFactory(poolConfig);
		streamTotalPrepaidAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_RealAmountByMonth",
								"plotId", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(totalTMFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		StateFactory totalYFactory = getStrFactory(poolConfig);
		streamTotalPrepaidAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_RealAmountByYear",
								"plotId", "year"), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(totalYFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		StateFactory factory = getStrFactory(poolConfig);
		streamTotalPrepaidAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_RealAmountByHidChargeNo", "plotId",
								"hid", "chargeNo", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(factory, new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		return topology.build();
	}
}
