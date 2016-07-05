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
 * @Description 本月补收(本月收以前月份的账单金额)
 * @author
 * @date 2016年3月30日 上午10:04:39
 * @update
 */
public class TotalIncomeAmountTopo extends AbstractBaseTopo {

	public TotalIncomeAmountTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topic = "vt_inc_bill_jour";
	public static final String topologyName = "Calc_TotalIncomeAmount_TOPO"
			+ new Random().nextInt(10000);
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new TotalIncomeAmountTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout("vt_inc_bill_jour");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		Stream streamTotalIncomeAmount = stream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig), new Fields(
						DASConstants.DATA)).parallelismHint(10);

		// -----#本月补收 根据小区/房号/日期 统计--------------------分割线
		StateFactory incByHidFactory = getStrFactory(poolConfig);
		streamTotalIncomeAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_IncomeAmountByHid",
								"plotId", "hid", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(incByHidFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		// -----#本月补收 根据小区/费项/日期 统计--------------------分割线
		StateFactory incByChargeNoFactory = getStrFactory(poolConfig);
		streamTotalIncomeAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_IncomeAmountByChargeNo",
								"plotId", "chargeNo", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(incByChargeNoFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		// -----求和
		StateFactory incByPlotIdFactory = getStrFactory(poolConfig);
		streamTotalIncomeAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_IncomeAmountByPlotId",
								"plotId", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(incByPlotIdFactory,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		StateFactory yearFac = getStrFactory(poolConfig);
		streamTotalIncomeAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_IncomeAmountByYear",
								"plotId", "year"), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(yearFac, new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		StateFactory chactory = getStrFactory(poolConfig);
		streamTotalIncomeAmount
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_IncomeAmountByHidChargeNo",
								"plotId", "hid", "chargeNo", "year", "month"),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(chactory, new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("businessAmount"));

		return topology.build();
	}

}
