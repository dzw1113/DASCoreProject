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
import com.icip.framework.filter.EqValBaseFilter;
import com.icip.framework.function.common.ConcatRedisKeyFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.spout.RedisPubSubSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/**
 * @Description 账单欠费(bs_bill_details)
 * @author
 * @date 2016年3月30日 上午10:04:39
 * @update
 */
public class TotalBillArreasAmountTopo extends AbstractBaseTopo {

	public TotalBillArreasAmountTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topic = "vt_bill";
	public static final String topologyName = "Calc_TotalBillArreasAmount_TOPO"
			+ new Random().nextInt(10000);
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new TotalBillArreasAmountTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout("vt_bill");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		Stream newStream = stream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig),
				new Fields(DASConstants.DATA)).parallelismHint(10).each(
				new Fields(DASConstants.DATA),
				new EqValBaseFilter("chargeStatus", "02"));

		StateFactory factory = getStrFactory(poolConfig);
		newStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_BillArreasAmountByHid",
								"plotId", "hid", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(factory, new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrreasAmount"),
						new Fields("arrreasAmount"));

		StateFactory arrByChargeNoFactory = getStrFactory(poolConfig);
		newStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_BillArreasAmountByChargeNo",
								"plotId", "chargeNo", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(arrByChargeNoFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrreasAmount"),
						new Fields("arrreasAmount"));

		StateFactory arrByPlotIdFactory = getStrFactory(poolConfig);
		newStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_BillArreasAmountByPlotId",
								"plotId", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(arrByPlotIdFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrreasAmount"),
						new Fields("arrreasAmount"));

		StateFactory yearFactory = getStrFactory(poolConfig);
		newStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_BillArreasAmountByYear",
								"plotId", "year"), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(yearFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrreasAmount"),
						new Fields("arrreasAmount"));

		StateFactory hcFactory = getStrFactory(poolConfig);

		newStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_BillArreasAmountByHidChargeNo", "plotId",
								"hid", "chargeNo", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(hcFactory, new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrreasAmount"),
						new Fields("arrreasAmount"));

		return topology.build();
	}

}
